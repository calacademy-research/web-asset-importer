import bs4

import time_utils
import subprocess
import base64
from bs4 import BeautifulSoup, NavigableString


def encode_image(image_path):
    with open(image_path, 'rb') as image_file:
        # Read the binary image data
        image_binary = image_file.read()

    image_base64 = base64.b64encode(image_binary).decode()

    return image_base64


def send_txt_to_email(subject, recipient, file):
    """send_txt_to_email: uses the subproccess package in order to send
       emails from command line.
       args:
            subject: the subject line of the email
            recipient: the email address of the recipient
            file: the text file to copy to an email
    """
    email_command = f'mutt -s "{subject}" -a {file} -- {recipient} < <(echo -e "batch report from ' \
                    f'{time_utils.get_pst_time_now_string()}")'
    try:
        subprocess.run(['bash', '-c', email_command])
        print(f"Email sent to {recipient} with subject: {subject}")
    except subprocess.CalledProcessError as e:
        print(f"Error sending email: {e}")


def clear_txt(path):
    """clears out the all the contents of a .txt file , leaving a blank file"""
    with open(path, 'w') as file:
        pass

def add_imagepath_to_txt(path, barcode, success):
    """add_filepath_to_monitor_txt: adds single line to end of txt file,
        in this case with 4 spaces,
        to keep alignment with generic template
        args:
            path: path to the txt file
            failure: indicates whether image at filepath failed to upload to image server or not
    """
    monitor_line = f"<tr><td>{path}</td> <td>{barcode}</td><td>{success}</td></tr>"

    # add_line_between(txt_file="import_monitoring.html", line_num=14, string=monitor_line)
    with open("import_monitoring.html", "r") as file:
        html_content = file.readlines()

    insert_position = len(html_content) - 6

    html_content.insert(insert_position, monitor_line + '\n')

    # Write the updated HTML content back to the file
    with open("import_monitoring.html", 'w') as file:
        file.writelines(html_content)

def add_line_between(txt_file, line_num, string):
    """add_line_between: used to add a string line into a txt file, between two existing lines,
       using a line index.
       args:
            txt_file: path to txt file to read
            line_num: the line number after which to insert a new line/lines of text
            string: the actual line of text you wish to insert."""
    with open(txt_file, "r") as file:
        lines = file.readlines()

    lines.insert(line_num, string + "\n")

    with open(txt_file, "w") as file:
        file.writelines(lines)

def create_summary_term_list(value_list, config):
    if value_list is None:
        return None
    else:
        terms = ""
        for index, term in enumerate(config.SUMMARY_TERMS):
            terms += f"<li>{term}: {value_list[index]}</li>"
        return terms
def add_format_batch_report(num_records, uploader, md5_code, config, custom_terms):
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
    report = """<html>
    <head>
        <title>Upload Batch Report</title>
            <style>
        table {
            border-collapse: collapse;
            width: 100%;
        }

        table, th, td {
            border: 1px solid black;
        }

        th, td {
            padding: 8px;
            text-align: center;
        }
    </style>
    </head>
    """ + f"""<body>
        <h1>Upload Batch Report</h1>
        <hr>
        <p>Date and Time: {time_utils.get_pst_time_now_string()}</p>
        <p>Uploader: {uploader}</p>
        <p>Batch md5: {md5_code}</p>

        <h2>Summary:</h2>
        <ul>
            <li>Number of Records Added: {num_records}</li>
            {custom_terms}
        </ul>
        <img src="tests/test_images/test_image.jpg" alt="picture of a woodshop" >
        <h2>Images Uploaded:</h2>
        <table>
            <tr>
                <th>File Path</th>
                <th>Barcode</th>
                <th>Failure</th>
            </tr>
            
            
        </table>
        
    </body>
    </html>
    """

    add_line_between("import_monitoring.html", line_num=0, string=report)


    for email in config.mailing_list:
        send_txt_to_email(subject=f"Batch Upload:{time_utils.get_pst_time_now_string()}", recipient=email,
                          file="import_monitoring.html")

def create_monitoring_report(batch_size, batch_md5, agent, config_file, value_list=None):
    """creates customizable report template, and then sends it the form of an email
        args:
            value_list: the list of values to use for custom terms.
            batch_size: the size of your upload batch.
            batch_md5: the md5 code of your upload batch
            agent_number: the agent id or name of person who ran the import script.
            config_file: the config file to use
            """
    custom_terms = create_summary_term_list(config=config_file, value_list=value_list)

    add_format_batch_report(num_records=batch_size,
                            md5_code=batch_md5, uploader=agent, config=config_file,
                            custom_terms=custom_terms)


# send_txt_to_email("asdasd", recipient="mdelaroca@calacademy.org", file="import_monitoring.html")

add_imagepath_to_txt(path="path/to/image", barcode="123124", success=True)