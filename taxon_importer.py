import pandas as pd
import numpy as np
from taxon_parse_utils import remove_qualifiers
from string_utils import str_to_bool
import logging
from sql_csv_utils import SqlCsvTools
from get_configs import get_config
import argparse
import sys
from datetime import datetime
from taxon_tools.BOT_TNRS import process_taxon_resolve
from gen_import_utils import unique_ordered_list, remove_two_index
import time_utils
from uuid import uuid4

class IncorrectTaxonError(Exception):
    pass


starting_time_stamp = datetime.now()

class TaxonomyImporter:
    """
    TaxonomyImporter:
    Cleaning and import for a taxonomy only dataframe. Includes cleaning and TNRS methods
    from picturae parser as well.

    Args:
        record_full (pd.DataFrame): working dataframe (will be modified in-place)
        tnrs_ignore (bool): whether to ignore TNRS warnings
        logging_level: logging level INFO, WARNING , DEBUG etc ....
    """

    def __init__(self, record_full: pd.DataFrame, config, logging_level, tnrs_ignore=False):
        self.record_full = record_full

        self.config = config


        self.logger = logging.getLogger(f"Client.{self.__class__.__name__}")
        self.logger.setLevel(logging_level)

        self.sql_csv_tools = SqlCsvTools(config=self.config, logging_level=self.logger.getEffectiveLevel())
        self.tnrs_ignore = tnrs_ignore


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

        if "CSV_batch" not in self.record_full.columns:
            self.record_full["CSV_batch"] = "batch_1"

    def taxon_concat(self, row):
        """taxon_concat:
                parses taxon columns to check taxon database, adds the Genus species, ranks, and Epithets,
                in the correct order, to create new taxon fullname in self.fullname. so that can be used for
                database checks.
            args:
                row: a row from a csv file containing taxon information with correct column names

        """
        hyb_index = self.record_full.columns.get_loc('Hybrid')
        is_hybrid = row.iloc[hyb_index]

        # defining empty strings for parsed taxon substrings
        full_name = ""
        tax_name = ""
        first_intra = ""
        gen_spec = ""
        hybrid_base = ""

        gen_index = self.record_full.columns.get_loc('Genus')
        genus = row.iloc[gen_index]

        column_sets = [
            ['Genus', 'Species', 'Rank 1', 'Epithet 1', 'Rank 2', 'Epithet 2'],
            ['Genus', 'Species', 'Rank 1', 'Epithet 1'],
            ['Genus', 'Species']
        ]

        for columns in column_sets:
            for column in columns:
                index = self.record_full.columns.get_loc(column)
                if pd.notna(row.iloc[index]) and row.iloc[index] != '':
                    if columns == column_sets[0]:
                        full_name += f" {row.iloc[index]}"
                    elif columns == column_sets[1]:
                        first_intra += f" {row.iloc[index]}"
                    elif columns == column_sets[2]:
                        gen_spec += f" {row.iloc[index]}"

        full_name = full_name.strip()
        first_intra = first_intra.strip()
        gen_spec = gen_spec.strip()

        # creating temporary string in order to parse taxon names without qualifiers
        separate_string = remove_qualifiers(full_name)
        taxon_strings = separate_string.split()

        second_epithet_in = row.iloc[self.record_full.columns.get_loc('Epithet 2')]
        first_epithet_in = row.iloc[self.record_full.columns.get_loc('Epithet 1')]
        spec_in = row.iloc[self.record_full.columns.get_loc('Species')]
        genus_in = row.iloc[self.record_full.columns.get_loc('Genus')]

        # changing name variable based on condition
        if pd.notna(second_epithet_in) and second_epithet_in != '':
            tax_name = remove_qualifiers(second_epithet_in)
        elif pd.notna(first_epithet_in) and first_epithet_in != '':
            tax_name = remove_qualifiers(first_epithet_in)
        elif pd.notna(spec_in) and spec_in != '':
            tax_name = remove_qualifiers(spec_in)
        elif pd.notna(genus_in) and genus_in != '':
            tax_name = remove_qualifiers(genus_in)
        else:
            return ValueError('missing taxon in row')

        if is_hybrid is True:
            if first_intra == full_name:
                if "var." in full_name or "subsp." in full_name or " f." in full_name or "subf." in full_name:
                    hybrid_base = full_name
                    full_name = " ".join(taxon_strings[:2])
                elif full_name != genus and full_name == gen_spec:
                    hybrid_base = full_name
                    full_name = taxon_strings[0]
                elif full_name == genus:
                    hybrid_base = full_name
                    full_name = full_name
                else:
                    self.logger.error('hybrid base not found')

            elif len(first_intra) != len(full_name):
                if "var." in full_name or "subsp." in full_name or " f." in full_name or "subf." in full_name:
                    hybrid_base = full_name
                    full_name = " ".join(taxon_strings[:4])
                else:
                    pass

        return str(gen_spec), str(full_name), str(first_intra), str(tax_name), str(hybrid_base)

    def missing_data_masks(self):
        """
        Taxonomy-only masks (subset of original):
        - missing_rank_csv: when both Rank 1 and Rank 2 are missing but an epithet is present
        - missing_family_csv: rows missing Family
        """
        rank1_missing = (self.record_full['Rank 1'].isna() | (self.record_full['Rank 1'] == '')) & \
                        (self.record_full['Epithet 1'].notna() & (self.record_full['Epithet 1'] != ''))

        rank2_missing = (self.record_full['Rank 2'].isna() | (self.record_full['Rank 2'] == '')) & \
                        (self.record_full['Epithet 2'].notna() & (self.record_full['Epithet 2'] != ''))

        missing_rank_csv = self.record_full.loc[rank1_missing & rank2_missing]

        missing_family = (self.record_full['Family'].isna() |
                          (self.record_full['Family'] == '') |
                          (self.record_full['Family'].isnull()))
        missing_family_csv = self.record_full.loc[missing_family]

        return missing_rank_csv, missing_family_csv

    def flag_missing_data(self):
        """
        Taxonomy-only: raise if there are missing ranks (both) or missing family.
        """
        missing_rank_csv, missing_family_csv = self.missing_data_masks()

        if len(missing_rank_csv) > 0:
            item_set = sorted(set(missing_rank_csv['folder_barcode']))
            batch_set = sorted(set(missing_rank_csv['CSV_batch']))
            raise ValueError(f"Taxonomic names with 2 missing ranks at covers: {item_set} in batches {batch_set}")

        if len(missing_family_csv) > 0:
            item_set = sorted(set(missing_family_csv['folder_barcode']))
            batch_set = sorted(set(missing_family_csv['CSV_batch']))
            raise ValueError(f"Rows missing taxonomic family at barcodes: {item_set} in batches {batch_set}")


    def col_clean(self):
        """parses and cleans dataframe columns until ready for upload.
            runs dependent function taxon concat
        """
        # converting hybrid column to true boolean
        self.record_full['Hybrid'] = self.record_full['Hybrid'].apply(str_to_bool)

        # removing leading and trailing space from taxa
        tax_cols = ['Genus', 'Species', 'Rank 1', 'Epithet 1', 'Rank 2', 'Epithet 2', 'Author', 'Family']
        existing_tax_cols = [c for c in tax_cols if c in self.record_full.columns]
        self.record_full[existing_tax_cols] = self.record_full[existing_tax_cols].map(
            lambda x: x.strip() if isinstance(x, str) else x
        )

        # filling in missing subtaxa ranks for first infraspecific rank
        self.record_full['missing_rank'] = (pd.isna(self.record_full[f'Rank 1']) & pd.notna(
                                           self.record_full[f'Epithet 1'])) | \
                                           ((self.record_full[f'Rank 1'] == '') & (self.record_full[f'Epithet 1'] != ''))

        self.record_full['missing_rank'] = self.record_full['missing_rank'].astype(bool)

        placeholder_rank = (pd.isna(self.record_full['Rank 1']) | (self.record_full['Rank 1'] == '')) & \
                           (self.record_full['missing_rank'] == True)

        # Set 'Rank 1' to 'subsp.' where the condition is True
        self.record_full.loc[placeholder_rank, 'Rank 1'] = 'subsp.'

        # parsing taxon columns into derived strings
        self.record_full[['gen_spec', 'fullname',
                          'first_intra',
                          'taxname', 'hybrid_base']] = self.record_full.apply(self.taxon_concat,
                                                                              axis=1, result_type='expand')

        # Keep strings for taxonomy columns; avoid global dtype casting to prevent side-effects
        for col in ['gen_spec', 'fullname', 'first_intra', 'taxname', 'hybrid_base']:
            self.record_full[col] = self.record_full[col].astype(str).replace(['', None, 'nan', np.nan], '')


    def taxon_process_row(self, row):
        """applies taxon_get to a row of the picturae python dataframe"""
        taxon_id = self.sql_csv_tools.taxon_get(
                        name=row['fulltaxon'],
                        hybrid=str_to_bool(row['Hybrid']),
                        taxname=row['taxname']
        )
        # Check for new genus to verify family assignment
        new_genus = False
        if taxon_id is None:
            genus_id = self.sql_csv_tools.taxon_get(
                name=row['Genus'],
                hybrid=str_to_bool(row['Hybrid']),
                taxname=row['taxname']
            )
            if genus_id is None:
                new_genus = True

        return taxon_id, new_genus


    def check_taxa_against_database(self):
        """check_taxa_against_database:
                concatenates every taxonomic column together to get the full taxonomic name,
                checks full taxonomic name against database and retrieves taxon_id if present
                and `None` if absent from db. In TNRS, only taxonomic names with a `None`
                result will be checked.
                args:
                    None
        """
        col_list = ['Genus', 'Species', 'Rank 1', 'Epithet 1', 'Rank 2', 'Epithet 2']

        # Build fulltaxon
        self.record_full['fulltaxon'] = (
            self.record_full[col_list].fillna('')
            .apply(lambda x: ' '.join(x[x != '']), axis=1)
            .str.strip()
        )

        # If empty or "missing taxon", fall back to Family
        self.record_full['fulltaxon'] = self.record_full.apply(
            lambda row: row['Family'] if (not row['fulltaxon'] or 'missing taxon' in row['fulltaxon'])
            else row['fulltaxon'],
            axis=1
        )

        # take the first occurrence of Hybrid/taxname/Genus per fulltaxon
        key_df = (
            self.record_full
            .loc[:, ['fulltaxon', 'Hybrid', 'taxname', 'Genus']]
            .dropna(subset=['fulltaxon'])
            .groupby('fulltaxon', as_index=False)
            .first()
        )

        # Apply your lookup exactly once per unique fulltaxon
        taxon_process_output = key_df.apply(lambda row: self.taxon_process_row(row), axis=1, result_type='expand')
        taxon_process_output.columns = ['taxon_id', 'new_genus']

        # Enforce unique index
        taxon_map_df = (
            key_df[['fulltaxon']]
            .join(taxon_process_output)
            .set_index('fulltaxon', verify_integrity=True)
        )

        # Factorize yields [0..n-1] in the order of appearance; use sort=True if you want alphabetical stability
        codes, uniques = pd.factorize(taxon_map_df.index, sort=True)
        taxon_map_df['fulltaxon_idx'] = codes  # Int64 dtype by default

        # Map the results back
        self.record_full['taxon_id'] = self.record_full['fulltaxon'].map(taxon_map_df['taxon_id'])
        self.record_full['new_genus'] = self.record_full['fulltaxon'].map(taxon_map_df['new_genus'])
        self.record_full['taxon_idx'] = self.record_full['fulltaxon'].map(taxon_map_df['fulltaxon_idx'])

        # Keep nullable Int64
        self.record_full['taxon_id'] = self.record_full['taxon_id'].astype(pd.Int64Dtype())
        self.record_full['taxon_idx'] = self.record_full['taxon_idx'].astype(pd.Int64Dtype())

        self.record_full.drop(columns=['fulltaxon'], inplace=True)


    def taxon_check_tnrs(self):
        """taxon_check_real:
           Sends the concatenated taxon column, through TNRS, to match names,
           with and without spelling mistakes, only checks base name
           for hybrids as IPNI does not work well with hybrids
           """
        from taxon_tools.BOT_TNRS import iterate_taxon_resolve  # local import to avoid hard dep at class load

        bar_tax = self.record_full[pd.isna(self.record_full['taxon_id']) | (self.record_full['taxon_id'] == '')]

        if len(bar_tax) <= 0:
            self.record_full['overall_score'] = 1
            self.record_full['name_matched'] = ''
            self.record_full['matched_name_author'] = ''

        elif len(bar_tax) >= 1:
            bar_tax = bar_tax[['CatalogNumber', 'fullname']]
            resolved_taxon = iterate_taxon_resolve(bar_tax)
            resolved_taxon.fillna({'overall_score': 0}, inplace=True)
            resolved_taxon = resolved_taxon.drop(columns=["fullname", "unmatched_terms"])

            # merging columns on Catalog Number
            if len(resolved_taxon) > 0:
                self.record_full = pd.merge(self.record_full, resolved_taxon, on="CatalogNumber", how="left")
            else:
                raise ValueError("resolved TNRS data not returned")

            self.cleanup_tnrs()
        else:
            self.logger.error("bar tax length non-numeric")


    def cleanup_tnrs(self):
        """cleanup_tnrs: operations to re-consolidate rows with hybrids parsed for tnrs,
            and rows with missing rank parsed for tnrs.
            Separates qualifiers into new column as well.
            note: Threshold of .99 is set so that it will flag any taxon that differs from its match in any way,
            which is why a second taxon-concat is not run.
        """

        # re-consolidating hybrid column to fullname and removing hybrid_base column
        self.record_full['hybrid_base'] = self.record_full['hybrid_base'].astype(str).str.strip()
        hybrid_mask = (self.record_full['hybrid_base'].notna()) & (self.record_full['hybrid_base'] != '')
        self.record_full.loc[hybrid_mask, 'fullname'] = self.record_full.loc[hybrid_mask, 'hybrid_base']
        self.record_full = self.record_full.drop(columns=['hybrid_base'])

        # consolidating taxonomy with replaced rank
        self.record_full['missing_rank'] = self.record_full['missing_rank'].replace({'True': True,
                                                                                     'False': False}).astype(bool)
        # mask for successful match
        good_match = (pd.notna(self.record_full['name_matched']) & self.record_full['name_matched'] != '') & \
                     (self.record_full['overall_score'] >= .99)
        # creating mask for missing ranks
        rank_mask = (self.record_full['missing_rank'] == True) & \
                    (self.record_full['fullname'] != self.record_full['name_matched']) & good_match

        # replacing good matches with their matched names
        self.record_full.loc[rank_mask, 'fullname'] = self.record_full.loc[rank_mask, 'name_matched']

        # replacing rank for missing rank cases in first intra and full taxon
        for col in ['fullname', 'first_intra']:
            self.record_full.loc[rank_mask, col] = \
                self.record_full.loc[rank_mask, col].str.replace(" subsp. ", " var. ", regex=False)

        for col in ['fullname', 'gen_spec', 'first_intra', 'taxname']:
            self.record_full[col] = self.record_full[col].apply(remove_qualifiers)

        # pulling new tax IDs for corrected missing ranks
        self.record_full.loc[rank_mask, 'taxon_id'] = self.record_full.loc[rank_mask, 'fullname'].apply(
            self.sql_csv_tools.taxon_get)

        if self.tnrs_ignore is False:
            self.flag_tnrs_rows()

    def flag_tnrs_rows(self):
        """function to flag TNRS rows that do not pass the .99 match threshold"""
        taxon_to_correct = self.record_full[(self.record_full['overall_score'] < 0.99) &
                                            (pd.notna(self.record_full['overall_score'])) &
                                            (self.record_full['overall_score'] != 0)]
        try:
            taxon_correct_table = taxon_to_correct[['CSV_batch', 'fullname',
                                                    'name_matched', 'overall_score']].drop_duplicates()
            assert len(taxon_correct_table) <= 0
        except:
            raise IncorrectTaxonError(f'TNRS has rejected taxonomic names at '
                                      f'the following batches: {taxon_correct_table}')


    #===============import_operations==============

    def init_all_vars(self):
        empty_lists = ['taxon_list', 'parent_list', 'new_taxa']

        for empty_list in empty_lists:
            setattr(self, empty_list, [])

        init_list = ['taxon_id', 'full_name', 'tax_name',
                     'determination_guid', 'name_id',
                     'author_sci', 'family', 'gen_spec_id',
                     'family_id', 'parent_author', 'redacted']

        for param in init_list:
            setattr(self, param, None)

        self.created_by_agent = self.config.IMPORTER_AGENT_ID



    def assign_col_dtypes(self):
        """just in case csv import changes column dtypes, resetting at top of file,
            re-standardizing null and nan records to all be pd.NA() and
            evaluate strings into booleans
        """
        # setting datatypes for columns
        string_list = self.record_full.columns.to_list()

        self.record_full[string_list] = self.record_full[string_list].astype(str)

        self.record_full = self.record_full.replace({'True': True, 'False': False}).infer_objects(copy=False)

        self.record_full = self.record_full.replace([None, 'nan', np.nan, '<NA>'], '')


    def taxon_assign_defitem(self, taxon_string):
        """taxon_assign_defitme: assigns, taxon rank and treeitemid number,
                                based on subtrings present in taxon name.
            args:
                taxon_string: the taxon string or substring, which before assignment
        """
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


    def taxa_author_tnrs(self, taxon_name, barcode):
        """taxa_author_tnrs: designed to take in one taxaname and
           do a TNRS operation on it to get an author for iterative higher taxa.

           args:
                taxon_name: a string of a taxon name, or a parsed genus or family, used to control
                            for unconfirmed species, and spelling mistakes.
                barcode: the string barcode of the taxon name associated with each photo.
                         used to re-merge dataframes after TNRS and keep track of the record in R.
        """

        taxon_frame = {"CatalogNumber": [barcode], "fullname": [taxon_name]}

        taxon_frame = pd.DataFrame(taxon_frame)

        # running taxonomic names through TNRS

        resolved_taxon = process_taxon_resolve(taxon_frame)

        taxon_list = list(resolved_taxon['matched_name_author'])

        self.parent_author = taxon_list[0]



    def populate_fields(self, row):
        """populate_fields:
               this populates all the
               initialized data fields per row for input into database,
               make sure to check column list is correct so that the
               row indexes are assigned correctly.
           args:
                row: a row from a botany specimen csv dataframe containing the required columns

        """
        self.barcode = row.CatalogNumber.zfill(9)
        self.full_name = row.fullname
        self.tax_name = row.taxname
        self.gen_spec = row.gen_spec
        self.qualifier = row.qualifier
        self.name_matched = row.name_matched
        self.genus = row.Genus
        self.family_name = row.Family
        self.is_hybrid = row.Hybrid
        self.author = row.matched_name_author
        self.first_intra = row.first_intra

        self.taxon_id = row.taxon_id

        self.overall_score = row.overall_score
        if hasattr(row, "cover_notes"):
            self.tax_notes = row.cover_notes
        else:
            self.tax_notes = ""


    def populate_taxon(self):
        """populate taxon: creates a taxon list, which checks different rank levels in the taxon,
                         as genus must be uploaded before species , before sub-taxa etc...
                         has cases for hybrid plants, uses regex to separate out sub-taxa hybrids,
                          uses parsed lengths to separate out genus level and species level hybrids.
                        cf. qualifiers already seperated, so less risk of confounding notations.
        """
        self.gen_spec_id = None
        self.taxon_list = []

        if self.is_hybrid is False and self.full_name == "missing taxon in row":
            self.taxon_id = self.sql_csv_tools.taxon_get(name=self.family_name)
            # will need to add a condition for project V2 to extract name for order or division
            if not self.taxon_id or pd.isna(self.taxon_id):
                raise ValueError(f"Family {self.family_name} not present in taxon tree")
                # self.taxon_list.append(self.family_name)

        elif not self.is_hybrid and self.full_name != "missing taxon in row":
            self.taxon_id = self.sql_csv_tools.taxon_get(name=self.full_name)
        else:
            self.taxon_id = self.sql_csv_tools.taxon_get(name=self.full_name,
                                                         taxname=self.tax_name, hybrid=True)

        # append taxon full name
        if not self.taxon_id or pd.isna(self.taxon_id):

            self.taxon_list.append(self.full_name)

            # check base name if base name differs e.g. if var. or subsp.
            if self.full_name != self.first_intra and self.first_intra != self.gen_spec:
                self.first_intra_id = self.sql_csv_tools.taxon_get(name=self.first_intra)
                if not self.first_intra_id or pd.isna(self.first_intra):
                    self.taxon_list.append(self.first_intra)

            if self.full_name != self.gen_spec and self.gen_spec != self.genus:
                self.gen_spec_id = self.sql_csv_tools.taxon_get(name=self.gen_spec)
                # check high taxa gen_spec for author
                self.taxa_author_tnrs(taxon_name=self.gen_spec, barcode=self.barcode)
                # adding base name to taxon_list
                if not self.gen_spec_id or pd.isna(self.gen_spec):
                    self.taxon_list.append(self.gen_spec)

                # base value for gen spec id is set as None so will work either way.
                # checking for genus id
            if self.full_name != self.genus:
                self.genus_id = self.sql_csv_tools.taxon_get(name=self.genus)
                # adding genus name if missing
                if not self.genus_id or pd.isna(self.genus_id):
                    self.taxon_list.append(self.genus)

            self.new_taxa.extend(self.taxon_list)
        else:
            pass


    def generate_taxon_fields(self, index, taxon):
        """generates necessary fields for creating new taxon fields
            args:
                index: index num in the taxon list.
                taxon: the taxon name in the taxon list ,
                       iterrated through from highest to lowest rank"""
        taxon_guid = uuid4()
        rank_name = taxon
        parent_id = self.sql_csv_tools.taxon_get(name=self.parent_list[index + 1])
        if taxon == self.full_name:
            rank_end = self.tax_name
        else:
            rank_end = taxon.split()[-1]

        author_insert = self.author

        if rank_name != self.family_name and rank_name != self.genus:
            tree_item_id, rank_id = self.taxon_assign_defitem(taxon_string=rank_name)
        elif rank_name == self.genus:
            rank_id = 180
            tree_item_id = 12
        else:
            rank_id = 140
            tree_item_id = 11

        if rank_id < 220 or (taxon == self.full_name and float(self.overall_score) < .90):
            author_insert = ''

        # assigning parent_author if needed , for gen_spec

        if rank_id == 220 and self.full_name != self.gen_spec:
            author_insert = self.parent_author

        if self.is_hybrid is True:
            author_insert = ''

        return author_insert, tree_item_id, rank_end, parent_id, taxon_guid, rank_id


    def create_taxon(self):
        """create_taxon: populates the taxon table iteratively by adding higher taxa first,
                         before lower taxa. Assigns taxa ranks and TaxonTreedefItemID.
                         Using parent list in order to populate parent ids, by using the parsed
                         rank levels of each taxon name.
        """
        self.parent_list = [self.full_name, self.first_intra, self.gen_spec, self.genus, self.family_name]
        self.parent_list = unique_ordered_list(self.parent_list)
        for index, taxon in reversed(list(enumerate(self.taxon_list))):
            # getting index pos of taxon in parent list

            author_insert, tree_item_id, rank_end, \
                parent_id, taxon_guid, rank_id = self.generate_taxon_fields(index=index, taxon=taxon)

            if self.sql_csv_tools.get_is_taxon_id_redacted(taxon_id=parent_id):
                self.redacted = True

            column_list = ['TimestampCreated',
                           'TimestampModified',
                           'Version',
                           'Author',
                           'FullName',
                           'GUID',
                           'Source',
                           'IsAccepted',
                           'IsHybrid',
                           'Name',
                           'RankID',
                           'TaxonTreeDefID',
                           'ParentID',
                           'ModifiedByAgentID',
                           'CreatedByAgentID',
                           'TaxonTreeDefItemID']

            value_list = [f"{time_utils.get_pst_time_now_string()}",
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
                          f"{tree_item_id}"
                          ]

            value_list, column_list = remove_two_index(value_list, column_list)

            sql_statement = self.sql_csv_tools.create_insert_statement(tab_name="taxon", col_list=column_list,
                                                                       val_list=value_list)

            self.sql_csv_tools.insert_table_record(sql=sql_statement.sql, params=sql_statement.params)

            logging.info(f"taxon: {taxon} created")

    def run_all(self):
        """
        Execute taxonomy cleaning in correct order; returns modified dataframe.
        """
        self.normalize_schema()
        self.flag_missing_data()
        self.col_clean()
        self.check_taxa_against_database()
        self.taxon_check_tnrs()
        self.init_all_vars()
        self.assign_col_dtypes()
        for row in self.record_full.itertuples(index=False):
            self.populate_fields(row)
            if not self.taxon_id or pd.isna(self.taxon_id):
                self.populate_taxon()
                if self.taxon_list:
                    self.create_taxon()


if __name__ == "__main__":
    def parse_bool(v: str) -> bool:
        if isinstance(v, bool):
            return v
        val = str(v).strip().lower()
        if val in {"true", "t", "1", "yes", "y"}:
            return True
        if val in {"false", "f", "0", "no", "n"}:
            return False
        raise argparse.ArgumentTypeError(f"Expected true/false, got: {v}")


    parser = argparse.ArgumentParser(
        description="Run taxonomy-only cleaning (TaxonCleaner) on a CSV."
    )

    parser.add_argument(
        "-v", "--verbose",
        action="count",
        default=0,
        help="Increase verbosity. -v = DEBUG. Default = INFO."
    )

    parser.add_argument(
        "--input",
        required=True,
        help="Path to input CSV containing taxonomy columns.",
    )

    parser.add_argument(
        "--config",
        required=True,
        help="Config name (string) used by get_config(), e.g. 'Botany_PIC'.",
    )
    parser.add_argument(
        "--tnrs_ignore",
        type=parse_bool,
        default=False,
        help="True/False to ignore TNRS warnings (default: False).",
    )

    args = parser.parse_args()

    # Load input CSV
    try:
        df = pd.read_csv(args.input)
    except Exception as e:
        logging.error(f"Failed to read input CSV '{args.input}': {e}")
        sys.exit(1)

    use_config = get_config(config=args.config)

    tax_importer = TaxonomyImporter(
        record_full=df,
        config=use_config,
        tnrs_ignore=args.tnrs_ignore,
        logging_level=args.verbose,
    )

    tax_importer.run_all()
