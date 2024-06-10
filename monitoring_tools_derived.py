from monitoring_tools import MonitoringTools
import time_utils
class MonitoringToolsDir(MonitoringTools):
    def __init__(self, batch_md5, config, report_path, active):
        self.batch_md5 = batch_md5
        MonitoringTools.__init__(self, config=config, report_path=report_path, active=active)

    def add_format_batch_report(self, custom_terms=None):
        """add_format_batch_report:
            creates template of upload batch report. Takes standard summary terms and args,
           and allows for custom terms to be added with custom_terms.
           args:
                num_records: the number of records uploaded to DB
                uploader: agent id , or name of uploader.
                md5_code: the md5 code of this current upload batch.
                config: the config file to use.
                custom_terms: the list of custom values to add as summary terms, myst correspond with order of
                              SUMMARY_TERMS variable in config."""

        if custom_terms is None:
            custom_terms = ""
        else:
            pass

        report = f"""<html>
        <head>
            <title>Upload Batch Report:</title>
            <style>
            """ + """
            img {
                max-width: 300px; /* Maximum width of 300 pixels */
                max-height: 300px; /* Maximum height of 200 pixels */
            }
                    table {
                table-layout: fixed;
                border-collapse: collapse;
                width: 100%;
            }

            table, th, td {
                border: 1px solid black;
            }

            th, td {
                padding: 8px;
                white-space: nowrap;
                overflow: hidden;
                text-align: left;
            }
        </style>
        </head>
        """ + f"""<body>
            <h1>Upload Batch Report</h1>
            <hr>
            <p>Date and Time: {time_utils.get_pst_time_now_string()}</p>
            <p>Batch MD5: {self.batch_md5}</p>
            <p>Uploader: {self.config.AGENT_ID}</p>

            <h2>Summary Statistics:</h2>
            <ul>
                {custom_terms}
            </ul>
            <h2>Summary Figures:</h2>
            <h2>Images Uploaded:</h2>
            <table>
                <tr>
                    <th style="width: 50%">File Path</th>
                    <th>Barcode</th>
                    <th>Success</th>
                </tr>

            </table>

        </body>
        </html>
        """

        self.add_line_between(line_num=0, string=report)


