
import time_utils
import subprocess
import base64
import os


def encode_image(image_path):
    with open(image_path, 'rb') as image_file:
        # Read the binary image data
        image_binary = image_file.read()

    image_base64 = base64.b64encode(image_binary).decode()

    return image_base64


def send_txt_to_email(subject, recipient):
    """send_txt_to_email: uses the subproccess package in order to send
       emails from command line.
       args:
            subject: the subject line of the email
            recipient: the email address of the recipient
            file: the text file to copy to an email
    """
    if not os.path.exists("import_monitoring.html"):
        subprocess.run("touch import_monitoring.html")

    email_command = f'mutt -s "{subject}" -a import_monitoring.html -- {recipient} < <(echo -e "batch report from ' \
                    f'{time_utils.get_pst_time_now_string()}")'
    try:
        subprocess.run(['bash', '-c', email_command])
        print(f"Email sent to {recipient} with subject: {subject}")
    except subprocess.CalledProcessError as e:
        print(f"Error sending email: {e}")


def send_out_emails(subject, config):
    """standard function for looping through mailing list in config file"""
    for email in config.mailing_list:
        send_txt_to_email(subject=subject, recipient=email)


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
    monitor_line = f"<tr style='width: 50%'><td>{path}</td> <td>{barcode}</td><td>{success}</td></tr>"

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

def add_format_batch_report(num_records, uploader, md5_code, custom_terms):
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
        <p>Uploader: {uploader}</p>
        <p>Batch md5: {md5_code}</p>

        <h2>Summary Statistics:</h2>
        <ul>
            <li>Number of Records Added: {num_records}</li>
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

    add_line_between("import_monitoring.html", line_num=0, string=report)


def create_monitoring_report(num_barcodes, batch_md5, agent, config_file, value_list=None):
    """creates customizable report template, and then sends it the form of an email
        args:
            value_list: the list of values to use for custom terms.
            batch_size: the size of your upload batch.
            batch_md5: the md5 code of your upload batch
            agent_number: the agent id or name of person who ran the import script.
            config_file: the config file to use
            """
    custom_terms = create_summary_term_list(config=config_file, value_list=value_list)

    add_format_batch_report(num_records=num_barcodes,
                            md5_code=batch_md5, uploader=agent,
                            custom_terms=custom_terms)


def convert_image_base64(image_path):
    with open(image_path, 'rb') as image_file:
        image_data = image_file.read()

        base64_data = base64.b64encode(image_data).decode('utf-8')

        return base64_data


def insert_images_to_html(image_path, title):
    """add_filepath_to_monitor_txt: adds single line to end of txt file,
        in this case with 4 spaces,
        to keep alignment with generic template
        args:
            path: path to the txt file
            failure: indicates whether image at filepath failed to upload to image server or not
    """

    with open("import_monitoring.html", "r") as file:
        html_content = file.readlines()

    base64_encoded = convert_image_base64(image_path)

    image_section = html_content.index('        <h2>Summary Figures:</h2>\n')

    image_html = f"<img src = 'data:image/jpg;base64,{base64_encoded}' alt = '{title}' " \
                 f"style='display:block' width='300'," \
                 f"height = '300' title='{title}'>"

    html_content.insert(image_section+1, image_html + '\n')

    # Write the updated HTML content back to the file
    with open("import_monitoring.html", 'w') as file:
        file.writelines(html_content)


# insert_images_to_html(image_path="tests/test_images/test_image.jpg")