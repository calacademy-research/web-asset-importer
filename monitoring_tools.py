import smtplib
import time_utils
from email.utils import make_msgid
from email.message import EmailMessage

def clear_txt(path):
    """clears out the all the contents of a text file , leaving a blank file.
        args:
            path: path of .txt or html file to clear"""
    with open(path, 'w') as file:
        pass


def add_imagepath_to_html(path, barcode, success):
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

def add_format_batch_report(num_records, uploader, md5_code, custom_terms=None):
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

def insert_cid_img(cid):
    """insert_cid_img: adds single line to end of txt file,
                        containing generic html embedded image with cid code.
        args:
            cid: the cid code to use for the embedded image html
    """

    with open("import_monitoring.html", "r") as file:
        html_content = file.readlines()

    image_section = html_content.index('        <h2>Summary Figures:</h2>\n')

    image_html = f"""<img src="cid:{cid[1:-1]}"
                    style="display:block" width="300" height="300">"""

    html_content.insert(image_section+1, image_html + '\n')

    # Write the updated HTML content back to the file
    with open("import_monitoring.html", 'w') as file:
        file.writelines(html_content)

def attach_html_images(config):
    """attach_html_images:
       an iterative function that allows for embedding of images
       in a variety of mail platforms, using both html and cids
       args:
        config: the config file to use containing image_paths
    """

    image_paths = config.SUMMARY_IMG
    msg = EmailMessage()
    image_cids = []

    for i in range(len(image_paths)):
        cid = make_msgid()
        insert_cid_img(cid)
        image_cids.append(cid)

    with open("import_monitoring.html", "r") as file:
        html_content = file.read()

    msg.add_alternative(html_content, subtype='html')

    for index, image in enumerate(image_paths):
        cid = image_cids[index]

        with open(f'{image}', 'rb') as img:
            msg.get_payload()[0].add_related(img.read(), 'image', 'jpeg', cid=cid)

    with open("import_monitoring.html", "w") as file:
        file.write(msg.as_string())

    return msg


def send_smtp_email(msg_string, to_email, config):
    """send_smtp_email: sends email through smtp server,
        using user credentials stored in config file
        args:
            to_email:recipient of email
            subject: subject line of email
            msg_string: content to be in body of email
            config: config file to send
            """
    with smtplib.SMTP(config.smtp_server, config.smtp_port) as server:
        server.starttls()
        server.login(config.smtp_user, config.smtp_password)
        server.sendmail(config.smtp_user, to_email, msg_string)

def send_monitoring_report(config, subject):
    """send_monitoring_repot: completes the final steps after adding batch failure/success rates.
                                attaches custom graphs and images before sending email through smtp
        args:
            config: config file to use for smtp credentials, and custom image list.
            subject: subject line of report email
    """
    msg = attach_html_images(config)
    for email in config.mailing_list:
        msg['Subject'] = subject
        msg['to'] = email
        msg_string = msg.as_string()
        send_smtp_email(msg_string=msg_string,
                        config=config, to_email=email)
