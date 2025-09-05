from monitoring_tools import MonitoringTools
import time_utils
import os
class MonitoringToolsDir(MonitoringTools):
    def __init__(self, batch_md5, config, report_path, active):
        self.batch_md5 = batch_md5
        MonitoringTools.__init__(self, config=config, report_path=report_path, active=active)


    def create_html_report(self, report_type: str, section_label: str):
        """
        Creates an HTML batch report (upload/remove) with standard structure.
        Args:
            report_type: The report type string, e.g. "Upload" or "Remove".
            section_label: Section heading inside the report, e.g. "Images Uploaded" or "Images Removed".
        """
        if not os.path.exists(self.path):
            os.makedirs(os.path.dirname(self.path), exist_ok=True)
            open(self.path, 'w').close()
        else:
            self.clear_txt()

        report = f"""<html>
        <head>
        <title>{report_type} Batch Report</title>
        <style>
            img {{
                max-width: 300px;
                max-height: 300px;
            }}
            table {{
                table-layout: fixed;
                border-collapse: collapse;
                width: 100%;
            }}
            table, th, td {{
                border: 1px solid black;
            }}
            th, td {{
                padding: 8px;
                white-space: nowrap;
                overflow: hidden;
                text-align: left;
            }}
        </style>
        </head>
        <body>
          <h1>{report_type} Batch Report</h1>
          <hr>
          <p>Date and Time: {time_utils.get_pst_time_now_string()}</p>
          <p>Batch MD5: {self.batch_md5}</p>
          <p>Uploader: {self.AGENT_ID}</p>
    
          <h2>{section_label}:</h2>
          <table>
              <tr>
                  <th style="width: 50%">File Path</th>
                  <th>ID</th>
                  <th>Success</th>
              </tr>
          </table>
        </body>
        </html>
        """

        self.add_line_between(line_num=0, string=report)