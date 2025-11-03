"""
Reduced taxonomy-only importer (simple input schema).

Input DataFrame columns expected (case-insensitive accepted):
  Family, Genus, Species, qualifier, Rank1, Epithet1, Rank2, Epithet2, ishybrid

Pipeline:
  - Normalize schema -> strings/bools
  - Parse taxon columns (gen_spec, fullname, first_intra, taxname, hybrid_base)
  - DB lookup to pre-fill taxon_id (unique per fulltaxon)
  - TNRS for unresolved names (iterate_taxon_resolve)
  - Clean/normalize TNRS outputs, set matched_name_author / overall_score
  - Create missing taxa from higher -> lower

Keeps original insert logic for RankID/TaxonTreedefItemID and author rules.
"""

import logging
from uuid import uuid4
from datetime import datetime

import pandas as pd
import numpy as np
from get_configs import get_config
from gen_import_utils import unique_ordered_list, remove_two_index
from sql_csv_utils import SqlCsvTools
import time_utils
import sys
import argparse
# parsing helpers & TNRS from your repo
from taxon_parse_utils import remove_qualifiers
from taxon_tools.BOT_TNRS import iterate_taxon_resolve, process_taxon_resolve

starting_time_stamp = datetime.now()


class PicturaeTaxonomyImporter:
    def __init__(self, config, record_full: pd.DataFrame):
        self.picturae_config = config
        self.logger = logging.getLogger(f"Client.{self.__class__.__name__}")

        # DB helper (Specify)
        self.sql_csv_tools = SqlCsvTools(
            config=self.picturae_config, logging_level=self.logger.getEffectiveLevel()
        )
        self.created_by_agent = self.picturae_config.IMPORTER_AGENT_ID

        # use provided DataFrame
        self.record_full = record_full.copy()

        # init state
        self.init_all_vars()
        self.normalize_schema()
        self.assign_col_dtypes()

        # parse + TNRS + create taxa
        self.parse_taxa_columns()
        self.check_taxa_against_database()
        self.tnrs_for_unresolved()
        self.run_all_methods()

    # ----------------------------
    # Init / schema / dtypes
    # ----------------------------
    def init_all_vars(self):
        self.new_taxa = []
        self.parent_author = ""
        self.taxon_id = None

        # row-scoped
        self.full_name = None
        self.tax_name = None
        self.gen_spec = None
        self.genus = None
        self.family_name = None
        self.is_hybrid = False
        self.author = ""
        self.first_intra = ""
        self.overall_score = 1.0

    def normalize_schema(self):
        """
        Accept flexible column capitalization/spacing and normalize to:
          Family, Genus, Species, qualifier, Rank 1, Epithet 1, Rank 2, Epithet 2, Hybrid
        Converts 'ishybrid' to boolean Hybrid if present.
        """
        colmap = {}
        lower_cols = {c.lower(): c for c in self.record_full.columns}

        def pick(*alts):
            for a in alts:
                if a.lower() in lower_cols:
                    return lower_cols[a.lower()]
            return None

        m = {
            "Family": pick("family"),
            "Genus": pick("genus"),
            "Species": pick("species"),
            "qualifier": pick("qualifier"),
            "Rank 1": pick("rank 1", "rank1"),
            "Epithet 1": pick("epithet 1", "epithet1"),
            "Rank 2": pick("rank 2", "rank2"),
            "Epithet 2": pick("epithet 2", "epithet2"),
            "Hybrid": pick("hybrid", "ishybrid"),
        }

        # create missing expected columns as empty strings
        for std, src in m.items():
            if src is None:
                self.record_full[std] = ""
            else:
                self.record_full.rename(columns={src: std}, inplace=True)

        # Coerce Hybrid
        self.record_full["Hybrid"] = (
            self.record_full["Hybrid"]
            .astype(str)
            .str.strip()
            .str.lower()
            .isin(["true", "t", "1", "yes", "y"])
        )

        # add an ID for TNRS merging if caller didn't provide one
        if "CatalogNumber" not in self.record_full.columns:
            self.record_full["CatalogNumber"] = (
                self.record_full.index.astype(str).str.zfill(9)
            )

    def assign_col_dtypes(self):
        """Normalize strings and nulls."""
        string_list = self.record_full.columns.to_list()
        self.record_full[string_list] = self.record_full[string_list].astype(str)
        # Put Hybrid back to bool after cast above
        self.record_full["Hybrid"] = self.record_full["Hybrid"].astype(str).str.lower().isin(
            ["true", "t", "1", "yes", "y"]
        )
        self.record_full = (
            self.record_full.replace({"True": True, "False": False})
            .infer_objects(copy=False)
            .replace([None, "nan", np.nan, "<NA>"], "")
        )

    # ----------------------------
    # Parsing (from picturae_csv_create.taxon_concat)
    # ----------------------------
    def _build_pieces(self, row) -> tuple[str, str, str, str, str]:
        """
        Re-implementation of taxon_concat for the simplified schema.

        Returns:
          gen_spec, full_name, first_intra, tax_name, hybrid_base
        """
        is_hybrid = bool(row.Hybrid)
        full_name = ""
        first_intra = ""
        gen_spec = ""
        hybrid_base = ""
        tax_name = ""

        # build strings by presence
        col_sets = [
            ["Genus", "Species", "Rank 1", "Epithet 1", "Rank 2", "Epithet 2"],
            ["Genus", "Species", "Rank 1", "Epithet 1"],
            ["Genus", "Species"],
        ]

        for cols in col_sets:
            s = []
            for c in cols:
                v = getattr(row, c, "")
                if pd.notna(v) and str(v).strip() != "":
                    s.append(str(v).strip())
            chunk = " ".join(s)
            if cols == col_sets[0]:
                full_name = chunk
            elif cols == col_sets[1]:
                first_intra = chunk
            else:
                gen_spec = chunk

        full_name = full_name.strip()
        first_intra = first_intra.strip()
        gen_spec = gen_spec.strip()

        # tax_name (base terminal epithet)
        second_ep = getattr(row, "Epithet 2", "")
        first_ep = getattr(row, "Epithet 1", "")
        spec = getattr(row, "Species", "")
        genus = getattr(row, "Genus", "")

        if second_ep:
            tax_name = remove_qualifiers(second_ep)
        elif first_ep:
            tax_name = remove_qualifiers(first_ep)
        elif spec:
            tax_name = remove_qualifiers(spec)
        elif genus:
            tax_name = remove_qualifiers(genus)
        else:
            tax_name = "missing taxon in row"

        # Hybrid handling: detect and set a hybrid_base if needed
        if is_hybrid:
            tokens = remove_qualifiers(full_name).split()
            if first_intra == full_name:
                if any(r in full_name for r in ["var.", "subsp.", " f.", "subf."]):
                    hybrid_base = full_name
                    full_name = " ".join(tokens[:2]) if len(tokens) >= 2 else full_name
                elif full_name != genus and full_name == gen_spec:
                    hybrid_base = full_name
                    full_name = tokens[0] if tokens else full_name
                elif full_name == genus:
                    hybrid_base = full_name
                else:
                    self.logger.error("hybrid base not found")
            elif len(first_intra) != len(full_name):
                if any(r in full_name for r in ["var.", "subsp.", " f.", "subf."]):
                    hybrid_base = full_name
                    full_name = " ".join(tokens[:4]) if len(tokens) >= 4 else full_name

        return str(gen_spec), str(full_name), str(first_intra), str(tax_name), str(hybrid_base)

    def parse_taxa_columns(self):
        """Vectorized parsing of simplified input -> derived fields."""
        # fill missing first-rank label if epithet present (matches your original default)
        rank_missing = (
            (self.record_full["Rank 1"].isna() | (self.record_full["Rank 1"] == ""))
            & (self.record_full["Epithet 1"].notna())
            & (self.record_full["Epithet 1"] != "")
        )
        self.record_full.loc[rank_missing, "Rank 1"] = "subsp."

        out = self.record_full.apply(
            lambda r: pd.Series(self._build_pieces(r), index=["gen_spec", "fullname", "first_intra", "taxname", "hybrid_base"]),
            axis=1,
        )
        self.record_full = pd.concat([self.record_full, out], axis=1)

        # normalize strings; strip qualifiers in working fields
        for col in ["fullname", "gen_spec", "first_intra", "taxname"]:
            self.record_full[col] = self.record_full[col].apply(remove_qualifiers)

        # ensure expected downstream columns exist
        if "matched_name_author" not in self.record_full.columns:
            self.record_full["matched_name_author"] = ""
        if "overall_score" not in self.record_full.columns:
            self.record_full["overall_score"] = ""

    # ----------------------------
    # DB lookups + TNRS
    # ----------------------------
    def _taxon_process_row(self, row):
        """Lookup taxon_id and flag if genus would be new."""
        taxon_id = self.sql_csv_tools.taxon_get(
            name=row["fulltaxon"], hybrid=bool(row["Hybrid"]), taxname=row["taxname"]
        )
        new_genus = False
        if taxon_id is None:
            genus_id = self.sql_csv_tools.taxon_get(
                name=row["Genus"], hybrid=bool(row["Hybrid"]), taxname=row["taxname"]
            )
            new_genus = genus_id is None
        return taxon_id, new_genus

    def check_taxa_against_database(self):
        """
        Build fulltaxon and pre-resolve taxon_id once per unique name,
        then map back to all rows.
        """
        parts = ["Genus", "Species", "Rank 1", "Epithet 1", "Rank 2", "Epithet 2"]
        self.record_full["fulltaxon"] = (
            self.record_full[parts].fillna("").apply(lambda x: " ".join(x[x != ""]), axis=1).str.strip()
        )
        # fallback to Family if empty
        self.record_full.loc[self.record_full["fulltaxon"] == "", "fulltaxon"] = self.record_full["Family"]

        key_df = (
            self.record_full.loc[:, ["fulltaxon", "Hybrid", "taxname", "Genus"]]
            .dropna(subset=["fulltaxon"])
            .drop_duplicates("fulltaxon")
        )

        # Per-unique lookup
        looked = key_df.apply(lambda r: pd.Series(self._taxon_process_row(r), index=["taxon_id", "new_genus"]), axis=1)
        taxon_map_df = key_df.join(looked).set_index("fulltaxon", verify_integrity=True)

        # Map back
        self.record_full["taxon_id"] = self.record_full["fulltaxon"].map(taxon_map_df["taxon_id"])
        self.record_full["new_genus"] = self.record_full["fulltaxon"].map(taxon_map_df["new_genus"])

        # tidy types
        try:
            self.record_full["taxon_id"] = self.record_full["taxon_id"].astype(pd.Int64Dtype())
        except Exception:
            pass

    def tnrs_for_unresolved(self):
        """Bulk TNRS for rows where taxon_id is still null/empty."""
        unresolved = self.record_full[self.record_full["taxon_id"].isna() | (self.record_full["taxon_id"] == "")]
        if len(unresolved) == 0:
            # no-op; set defaults for completeness
            self.record_full["overall_score"] = self.record_full.get("overall_score", 1)
            self.record_full["name_matched"] = self.record_full.get("name_matched", "")
            self.record_full["matched_name_author"] = self.record_full.get("matched_name_author", "")
            return

        send = unresolved[["CatalogNumber", "fullname"]].copy()
        resolved = iterate_taxon_resolve(send)

        if resolved is None or len(resolved) == 0:
            self.logger.warning("TNRS returned no rows; skipping TNRS merge.")
            return

        resolved.fillna({"overall_score": 0}, inplace=True)
        resolved = resolved.drop(columns=["fullname", "unmatched_terms"], errors="ignore")

        # merge back on CatalogNumber
        self.record_full = pd.merge(self.record_full, resolved, on="CatalogNumber", how="left", suffixes=("", "_tnrs"))

        # if TNRS yields author/score, keep them
        if "matched_name_author_tnrs" in self.record_full.columns:
            self.record_full["matched_name_author"] = self.record_full["matched_name_author"].where(
                self.record_full["matched_name_author"] != "", self.record_full["matched_name_author_tnrs"]
            )
            self.record_full.drop(columns=["matched_name_author_tnrs"], inplace=True, errors="ignore")

        if "overall_score_tnrs" in self.record_full.columns:
            self.record_full["overall_score"] = self.record_full["overall_score"].where(
                self.record_full["overall_score"] != "", self.record_full["overall_score_tnrs"]
            )
            self.record_full.drop(columns=["overall_score_tnrs"], inplace=True, errors="ignore")

        # If TNRS gave us a high-confidence match and we had a missing first-rank, reconcile as in original
        good_match = (
            (self.record_full.get("name_matched", "") != "")
            & (pd.to_numeric(self.record_full.get("overall_score", 0), errors="coerce").fillna(0) >= 0.99)
        )

        # If we ever carried a hybrid_base, collapse fullname back to hybrid base
        hb = self.record_full.get("hybrid_base", "")
        hybrid_mask = (hb != "") & pd.notna(hb)
        self.record_full.loc[hybrid_mask, "fullname"] = self.record_full.loc[hybrid_mask, "hybrid_base"]
        self.record_full.drop(columns=["hybrid_base"], inplace=True, errors="ignore")

        # Final safety: strip qualifiers again
        for col in ["fullname", "gen_spec", "first_intra", "taxname"]:
            if col in self.record_full.columns:
                self.record_full[col] = self.record_full[col].apply(remove_qualifiers)

    # ----------------------------
    # Original taxonomy creation logic
    # ----------------------------
    def taxon_assign_defitem(self, taxon_string: str):
        def_tree = 13
        rank_id = 220
        if "subsp." in taxon_string:
            def_tree = 14
            rank_id = 230
        if "var." in taxon_string:
            def_tree = 15
            rank_id = 240
        if "subvar." in taxon_string:
            def_tree = 16
            rank_id = 250
        if " f. " in taxon_string:
            def_tree = 17
            rank_id = 260
        if "subf." in taxon_string:
            def_tree = 21
            rank_id = 270
        return def_tree, rank_id

    def taxa_author_tnrs(self, taxon_name: str, barcode: str):
        taxon_frame = pd.DataFrame({"CatalogNumber": [barcode], "fullname": [taxon_name]})
        resolved_taxon = process_taxon_resolve(taxon_frame)
        taxon_list = list(resolved_taxon.get("matched_name_author", []))
        self.parent_author = taxon_list[0] if taxon_list else ""

    def populate_fields(self, row):
        self.barcode = str(row.CatalogNumber).zfill(9)
        self.full_name = row.fullname
        self.tax_name = row.taxname
        self.gen_spec = row.gen_spec
        self.qualifier = getattr(row, "qualifier", "")
        self.name_matched = getattr(row, "name_matched", "")
        self.genus = row.Genus
        self.family_name = row.Family
        self.is_hybrid = bool(row.Hybrid)
        self.matched_author = getattr(row, "matched_name_author", "")
        self.author = getattr(row, "Author", "")
        self.first_intra = row.first_intra
        try:
            self.overall_score = float(getattr(row, "overall_score", 1.0))
        except Exception:
            self.overall_score = 1.0

        # Resolve taxon id again if needed (post-TNRS)
        if self.is_hybrid:
            self.taxon_id = self.sql_csv_tools.taxon_get(
                name=self.full_name, taxname=self.tax_name, hybrid=True
            )
        elif self.full_name == "missing taxon in row":
            self.taxon_id = self.sql_csv_tools.taxon_get(name=self.family_name)
        else:
            self.taxon_id = self.sql_csv_tools.taxon_get(name=self.full_name)

    def populate_taxon(self):
        self.gen_spec_id = None
        self.taxon_list = []

        if self.taxon_id and not pd.isna(self.taxon_id):
            return

        self.taxon_list.append(self.full_name)

        if self.full_name != self.first_intra and self.first_intra != self.gen_spec:
            self.first_intra_id = self.sql_csv_tools.taxon_get(name=self.first_intra)
            if not self.first_intra_id or pd.isna(self.first_intra_id):
                self.taxon_list.append(self.first_intra)

        if self.full_name != self.gen_spec and self.gen_spec != self.genus:
            self.gen_spec_id = self.sql_csv_tools.taxon_get(name=self.gen_spec)
            self.taxa_author_tnrs(taxon_name=self.gen_spec, barcode=self.barcode)
            if not self.gen_spec_id or pd.isna(self.gen_spec_id):
                self.taxon_list.append(self.gen_spec)

        if self.full_name != self.genus:
            self.genus_id = self.sql_csv_tools.taxon_get(name=self.genus)
            if not self.genus_id or pd.isna(self.genus_id):
                self.taxon_list.append(self.genus)

        self.new_taxa.extend(self.taxon_list)

    def generate_taxon_fields(self, index: int, taxon: str):
        taxon_guid = uuid4()
        rank_name = taxon
        parent_id = self.sql_csv_tools.taxon_get(name=self.parent_list[index + 1])
        rank_end = self.tax_name if taxon == self.full_name else taxon.split()[-1]

        if not self.author:
            author_insert = self.matched_author
        else:
            author_insert = self.author

        if rank_name not in (self.family_name, self.genus):
            tree_item_id, rank_id = self.taxon_assign_defitem(taxon_string=rank_name)
        elif rank_name == self.genus:
            rank_id = 180
            tree_item_id = 12
        else:
            rank_id = 140
            tree_item_id = 11

        if rank_id < 220 or (taxon == self.full_name and float(self.overall_score) < 0.90):
            author_insert = ""

        if rank_id == 220 and self.full_name != self.gen_spec:
            author_insert = self.parent_author

        if self.is_hybrid or rank_id < 220:
            author_insert = ""

        return author_insert, tree_item_id, rank_end, parent_id, taxon_guid, rank_id

    def create_taxon(self):
        self.parent_list = [self.full_name, self.first_intra, self.gen_spec, self.genus, self.family_name]
        self.parent_list = unique_ordered_list(self.parent_list)

        for index, taxon in reversed(list(enumerate(self.taxon_list))):
            author_insert, tree_item_id, rank_end, parent_id, taxon_guid, rank_id = \
                self.generate_taxon_fields(index=index, taxon=taxon)

            column_list = [
                "TimestampCreated",
                "TimestampModified",
                "Version",
                "Author",
                "FullName",
                "GUID",
                "Source",
                "IsAccepted",
                "IsHybrid",
                "Name",
                "RankID",
                "TaxonTreeDefID",
                "ParentID",
                "ModifiedByAgentID",
                "CreatedByAgentID",
                "TaxonTreeDefItemID",
            ]

            value_list = [
                f"{time_utils.get_pst_time_now_string()}",
                f"{time_utils.get_pst_time_now_string()}",
                1,
                author_insert,
                f"{taxon}",
                f"{taxon_guid}",
                "World Checklist of Vascular Plants 2023",
                True,
                self.is_hybrid,
                f"{rank_end}",
                f"{rank_id}",
                1,
                f"{parent_id}",
                f"{self.created_by_agent}",
                f"{self.created_by_agent}",
                f"{tree_item_id}",
            ]

            value_list, column_list = remove_two_index(value_list, column_list)
            stmt = self.sql_csv_tools.create_insert_statement(
                tab_name="taxon", col_list=column_list, val_list=value_list
            )
            self.sql_csv_tools.insert_table_record(sql=stmt.sql, params=stmt.params)
            self.logger.info(f"taxon created: {taxon}")

    # ----------------------------
    # Runner
    # ----------------------------
    def run_all_methods(self):
        # If the caller provided duplicate CatalogNumbers, reduce churn
        if "CatalogNumber" in self.record_full.columns:
            self.record_full = self.record_full.drop_duplicates(subset=["CatalogNumber"])

        for row in self.record_full.itertuples(index=False):
            self.populate_fields(row)
            if not self.taxon_id or pd.isna(self.taxon_id):
                self.populate_taxon()
                if self.taxon_list:
                    self.create_taxon()

        self.logger.info("taxonomy import finished")


def main():
    parser = argparse.ArgumentParser(
        description="Run taxonomy-only import from a simplified CSV."
    )
    parser.add_argument("input_csv", help="Path to input CSV file.")
    parser.add_argument(
        "config",
        help="Config key to resolve with get_config (e.g., 'Botany_PIC').",
    )

    args = parser.parse_args()

    # Basic logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    # Load config module
    cfg = get_config(args.config)
    if cfg is None:
        print(f"Error: config '{args.config}' could not be loaded.", file=sys.stderr)
        sys.exit(2)

    # Read CSV
    try:
        df = pd.read_csv(args.input_csv)
    except Exception as e:
        print(f"Error reading CSV '{args.input_csv}': {e}", file=sys.stderr)
        sys.exit(2)

    try:
        PicturaeTaxonomyImporter(config=cfg, record_full=df)
    except Exception as e:
        logging.exception("Import failed")
        print(f"Import failed: {e}", file=sys.stderr)
        sys.exit(1)

    print("Import completed successfully.")

if __name__ == "__main__":
    main()