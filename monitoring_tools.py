import logging
import os.path

import pandas as pd
import time_utils
from email.utils import make_msgid
from email.message import EmailMessage
from sql_csv_utils import SqlCsvTools
import smtplib

class MonitoringTools:
    def __init__(self, config, report_path, active=False):

        self.path = report_path
        self.config = config
        self.logger = logging.getLogger(f'Client.' + self.__class__.__name__)

        if active is True:
            self.AGENT_ID = self.config.IMPORTER_AGENT_ID
        else:
            self.AGENT_ID = self.config.AGENT_ID

        if not pd.isna(config) and config != {}:
            self.check_config_present()
            self.sql_csv_tools = SqlCsvTools(config=self.config, logging_level=self.logger.getEffectiveLevel())

    def clear_txt(self):
        """clears out the all the contents of a text file , leaving a blank file.
            args:
            args:
                path: path of .txt or html file to clear"""
        with open(self.path, 'w') as file:
            pass

    def check_config_present(self):
        """checks if mandatory config terms present in config file for email"""
        required_terms = ['SUMMARY_IMG', 'SUMMARY_TERMS',
                          'MAILING_LIST']
        for term in required_terms:
            if not hasattr(self.config, term):
                raise ValueError(f"Config is missing term '{term}'")

    def add_imagepaths_to_html(self, image_dict):
        """add_imagepaths_to_html: adds an image path row to the HTML table before the closing </table> tag.
        Args:
            image_dict: dictionary where the key is the image ID and value is a list of tuples
                        containing image paths and their success status.
        """
        for key, value in image_dict.items():
            img_id = key
            for result in value:
                image_path = result[0]
                success = result[1]
                monitor_line = f"<tr style='width: 50%'><td>{image_path}</td> <td>{img_id}</td><td>{success}</td></tr>"

                with open(self.path, "r") as file:
                    html_content = file.readlines()

                insert_position = None

                # Find the position of the closing </table> tag
                for i, line in enumerate(html_content):
                    if '</table>' in line:
                        insert_position = i
                        break

                # Insert the monitor line before the closing </table> tag
                if insert_position is not None:
                    html_content.insert(insert_position, monitor_line + '\n')
                    with open(self.path, 'w') as file:
                        file.writelines(html_content)
                else:
                    raise ValueError("Closing </table> tag not found in the HTML file.")

    def add_line_between(self, line_num: int, string: str):
        """add_line_between: used to add a string line into a txt file, between two existing lines,
           using a line index.
           args:
                line_num: the line number after which to insert a new line/lines of text
                string: the actual line of text you wish to insert."""
        with open(self.path, "r") as file:
            lines = file.readlines()

        lines.insert(line_num, string + "\n")

        with open(self.path, "w") as file:
            file.writelines(lines)

    def create_summary_term_list(self, value_list):
        """function that adds in list of custom summary stats from config file
            args:
                value_list = list of values to give to
                            summary terms calculated during import
        """
        if not value_list:
            return None
        else:
            terms = ""
            for index, term in enumerate(self.config.SUMMARY_TERMS):
                terms += f"<li>{term}: {value_list[index]}</li>\n"
            return terms
    @staticmethod
    def append_monitoring_dict(monitoring_dict, unique_id, original_path, success, logger=None):
        """append_monitoring_dict: adds paths to monitoring dictionary,
           allows for multiple original paths to be added per ID without replacement.
        """
        if unique_id in monitoring_dict:
            path_exists = any(original_path == item[0] for item in monitoring_dict[unique_id])

            if not path_exists:
                # If original_path is not in the list of lists, append the new list
                monitoring_dict[unique_id].append([original_path, success])
            if logger:
                logger.error(f"Attemping to upload {original_path} twice for {unique_id}")
        else:
            # If id does not exist, create a new list of lists
            monitoring_dict[unique_id] = [[original_path, success]]

    def create_monitoring_report(self):
        """add_format_batch_report:
            creates template of upload batch report. Takes standard summary terms and args,
           and allows for custom terms to be added with custom_terms.
           args:
                custom_terms: the list of custom values to add as summary terms, myst correspond with order of
                              SUMMARY_TERMS variable in config."""
        if not os.path.exists(path=self.path):
            os.makedirs(os.path.dirname(self.path), exist_ok=True)

            open(self.path, 'w').close()
        else:
            self.clear_txt()

        report = """<html>
                    <head>
                    <title>Upload Batch Report</title>
                    <style>
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
                          <p>Uploader: {self.AGENT_ID}</p>

                          <h2>Images Uploaded:</h2>
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

    def add_batch_size(self, batch_size):
        with open(self.path, "r") as file:
            html_content = file.readlines()

        list_section = next((i for i, s in enumerate(html_content) if "<ul>" in s), None)

        image_html = f"""    <li>Number of Images Added: {batch_size} </li>"""

        html_content.insert(list_section + 1, image_html + '\n')

        # Write the updated HTML content back to the file
        with open(self.path, 'w') as file:
            file.writelines(html_content)

    def add_summary_statistics(self, value_list):
        """Adds the summary statistics to the report after the uploader information.
            args:
                custom_terms: the list of custom values to add as summary terms.
        """
        if value_list:
            custom_terms = self.create_summary_term_list(value_list=value_list)

        with open(self.path, "r") as file:
            html_content = file.readlines()

        # Find the position of the <p>Uploader: {self.AGENT_ID}</p> line
        uploader_idx = next((i for i, line in enumerate(html_content) if f"<p>Uploader: {self.AGENT_ID}</p>" in line),
                            None)

        if uploader_idx is not None:
            # Insert the custom terms 2 lines below the uploader line
            insert_idx = uploader_idx + 2
            html_content.insert(insert_idx, f"<h2>Summary Statistics:</h2>\n")
            if value_list:
                html_content.insert(insert_idx + 1, f"<ul>{custom_terms}</ul>\n")
            else:
                html_content.insert(insert_idx + 1, f"<ul></ul>\n")

            # Write the updated HTML content back to the file
            with open(self.path, 'w') as file:
                file.writelines(html_content)

    def insert_cid_img(self, cid):
        """insert_cid_img: adds single line to end of txt file,
                            containing generic html embedded image with cid code.
            args:
                cid: the cid code to use for the embedded image html
        """

        with open(self.path, "r") as file:
            html_content = file.readlines()

        image_section = next((i for i, s in enumerate(html_content) if "Summary Figures:" in s), None)

        image_html = f"""<img src="cid:{cid[1:-1]}"
                        style="display:block" width="300" height="300">"""

        html_content.insert(image_section + 1, image_html + '\n')

        # Write the updated HTML content back to the file
        with open(self.path, 'w') as file:
            file.writelines(html_content)

    def attach_html_images(self):
        """attach_html_images:
           an iterative function that allows for embedding of images
           in a variety of mail platforms, using both html and cids
        """
        msg = EmailMessage()
        if not self.config.SUMMARY_IMG:
            image_paths = self.config.SUMMARY_IMG
            image_cids = []
            for i in range(len(image_paths)):
                cid = make_msgid()
                self.insert_cid_img(cid)
                image_cids.append(cid)

        with open(self.path, "r") as file:
            html_content = file.read()

        msg.add_alternative(html_content, subtype='html')

        if not self.config.SUMMARY_IMG:
            for index, image in enumerate(self.config.SUMMARY_IMG):
                cid = image_cids[index]
                with open(f'{image}', 'rb') as img:
                    msg.get_payload()[0].add_related(img.read(), 'image', 'jpeg', cid=cid)

        with open(self.path, "w") as file:
            file.write(msg.as_string())

        return msg

    def send_monitoring_report(self, subject, time_stamp, image_dict: dict, value_list=None):
        """send_monitoring_report: completes the final steps after adding batch failure/success rates.
                                    attaches custom graphs and images before sending email through smtp
            args:
                subject: subject line of report email
                time_stamp: the starting timestamp for upload
        """
        self.create_monitoring_report()
        self.add_imagepaths_to_html(image_dict)

        self.add_summary_statistics(value_list)

        sql = f"""SELECT COUNT(*)
                          FROM attachment
                          WHERE TimestampCreated >= %s 
                          AND CreatedByAgentID = %s ;"""

        params = (f'{str(time_stamp)}', f'{self.AGENT_ID}')

        self.sql_csv_tools.ensure_db_connection()
        batch_size = self.sql_csv_tools.get_record(sql=sql, params=params)

        if batch_size is None:
            self.logger.warning("batch_size is None. If not true, check configured AgentID")
            batch_size = 0
        if batch_size > 0:
            self.add_batch_size(batch_size=batch_size)
            msg = self.attach_html_images()
            msg['From'] = "ibss-central@calacademy.org"
            msg['Subject'] = subject
            recipient_list = []
            for email in self.config.MAILING_LIST:
                recipient_list.append(email)
            msg['To'] = recipient_list

            with smtplib.SMTP('localhost') as server:
                server.send_message(msg)
