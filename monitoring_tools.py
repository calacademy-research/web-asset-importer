
import time_utils
import subprocess

def send_txt_to_email(subject, recipient, file):
    """send_txt_to_email: uses the subproccess package in order to send
       emails from command line.
       args:
            subject: the subject line of the email
            recipient: the email address of the recipient
            file: the text file to copy to an email
    """
    email_command = f'cat {file} | mail -s "{subject}" {recipient}'
    try:
        subprocess.run(email_command, shell=True, check=True)
        print(f"Email sent to {recipient} with subject: {subject}")
    except subprocess.CalledProcessError as e:
        print(f"Error sending email: {e}")


def clear_txt(path):
    """clears out the all the contents of a .txt file , leaving a blank file"""
    with open(path, 'w') as file:
        pass

def add_imagepath_to_txt(path, failure=False):
    """add_filepath_to_monitor_txt: adds single line to end of txt file,
        in this case with 4 spaces,
        to keep alignment with generic template
        args:
            path: path to the txt file
            failure: indicates whether image at filepath failed to upload to image server or not
    """
    if failure is True:
        monitor_line = " "*4 + f"{path} -- Upload Failure"
    else:
        monitor_line = " "*4 + f"{path}"
    with open("import_monitoring.txt", "a") as file:
        file.write(f'{monitor_line}\n')

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
    """create_summary_term_list: parses list of custom summary terms to add to template.
        value list: the list of values to assign to each custom term, in order.
        config: the config file from which to get the SUMMARY_TERMS list.
    """
    if value_list is None:
        return None
    else:
        terms = ""
        for index, term in enumerate(config.SUMMARY_TERMS):
            terms += f"""- {term}:{value_list[index]}\n""" + " "*4
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

    report = f"""Upload Batch Report:
    -------------------

    Date and Time: {time_utils.get_pst_time_now_string()}
    Uploader: {uploader}
    Batch md5: {md5_code}

    Summary:
    - Number of Records Added: {num_records}
    {custom_terms}
    
    Images_Uploaded:
    """

    add_line_between("import_monitoring.txt", line_num=0, string=report)


    for email in config.mailing_list:
        send_txt_to_email(subject=f"Batch Upload:{time_utils.get_pst_time_now_string()}", recipient=email,
                          file="import_monitoring.txt")

def create_monitoring_report(batch_size, batch_md5, agent_number, config_file, value_list=None):
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
                            md5_code=batch_md5, uploader=agent_number, config=config_file,
                            custom_terms=custom_terms)