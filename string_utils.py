"""Docstring: This is a utility file, outlining various useful functions for parsing and cleaning strings
"""
import pandas as pd
import re
pd.set_option('expand_frame_repr', False)


# String and numeric reformating tools

def str_to_bool(value: str):
    """str_to_bool: will take a column with string 'True' & 'False' and convert to true booleans.
                    Most useful when used with .apply function

        args:
            value: string value to convert to boolean
        returns:
            boolean conversion of string value.
    """
    if isinstance(value, bool):
        return value
    else:
        value = str(value)
        return value.lower() == 'true' or value.lower() == "t"


def remove_non_numerics(string: str):
    """remove_non_numerics: A small data_cleaning function to remove numerics from a string
    args:
        string: representing a string
    returns:
        re.sub: a string with only numerics, use .apply on dataframe objects"""
    return re.sub('[^0-9]+', '', string)


def escape_apostrophes(string: str, reverse=False):
    """
    Replaces single apostrophes with double apostrophes for MySQL compatibility,
    but skips already escaped double apostrophes.

    Args:
        string: A string that may contain apostrophes.
        reverse: to replace pre-escaped apostrophes in reverse with single apostrophes

    Returns:
        str: A string with unescaped apostrophes replaced by double apostrophes.
    """
    if isinstance(string, str) and reverse is False:
        # Replace single apostrophes not preceded by another apostrophe
        return re.sub(r"(?<!')'", "''", string)
    elif isinstance(string, str) and reverse is True:
        # replace only even pairs of apostrophes
        return re.sub(r"(?<!')''(?!')", "'", string)
    else:
        return string



def move_first_substring(string: str, n_char: int):
    """move_first_substring: will move first n letters from beginning to end of string
       args:
            string: any string
        returns:
            string: a string the first n characters moved to end
        """
    if len(string) <= n_char:
        return string
    else:
        return string[n_char+1:] + string[0:n_char+1]


def remove_barcode_suffix(num):
    """Remove barcode notation of _ followed by any number for duplicate record sheets."""
    if isinstance(num, int):
        num = str(num)
    return re.sub(r'_\d+$', '', num)

def assign_collector_titles(first_last, name: str, config):
    """assign_titles:
            function designed to separate out titles in names into a new title column
        args:
            first_last: whether the name is a first or a last name with string 'first' 'last'
            name: the name string from which to separate out the titles.
    """
    # to lower to standardize matching
    first_name_titles = config.AGENT_FIRST_TITLES
    last_name_titles = config.AGENT_LAST_TITLES
    title = ""
    new_name = name
    # Split the full name into words
    if name and pd.notna(name) and name != '':
        name_parts = name.split()
    # Find the title in the name_parts
        if name_parts:
            if first_last == "first" and (name_parts[0].lower() in first_name_titles):
                new_name = " ".join(name_parts[1:])
                title = name_parts[0]

            elif first_last == "last" and (name_parts[-1].lower() in last_name_titles):
                new_name = " ".join(name_parts[:-1])
                title = name_parts[-1]
            else:
                # If no title is found, assign the full name to the first name
                new_name = name

    return new_name, title

def roman_to_int(string):

    """convert_roman_numeral: takes a string, and replaces roman numerals with integer.
                                Be careful of strings with non-numeral capital Is, Vs etc..
       args:
            string: any string with roman numerals, can be used to loop through elements in a list or vector.
        returns:
            output: a string where all roman numerals are replaced with integers."""

    roman_numerals = {'I': 1, 'V': 5, 'X': 10,
                      'L': 50, 'C': 100, 'D': 500, 'M': 1000}
    output = ''
    i = 0

    while i < len(string):
        if string[i] in roman_numerals:
            numeral_start = i
            while i < len(string) and string[i] in roman_numerals:
                i += 1
            numeral_end = i
            roman_numeral = string[numeral_start:numeral_end]
            integer_value = 0
            for j in range(len(roman_numeral)):
                if j + 1 < len(roman_numeral) and roman_numerals[roman_numeral[j]] < \
                           roman_numerals[roman_numeral[j + 1]]:

                    integer_value -= roman_numerals[roman_numeral[j]]
                else:
                    integer_value += roman_numerals[roman_numeral[j]]
            output += str(integer_value)

        else:
            output += string[i]
            i += 1

    return output


def string_to_int_converter(df: pd.DataFrame, column: str, option: str):
    """function to turn string with decimal points into string or int with no decimals
       args:
            df: dataframe to modify
            column: string name of column to modify
            option: end result output
        returns:
            df: a dataframe with the modified column
    """
    if option == "str":
        df[column] = df[column].fillna(0)
        df[column] = pd.to_numeric(df[column], errors='coerce')
        df[column] = df[column].astype(int).astype(str)
        return df
    elif option == "int":
        df[column] = df[column].fillna(0)
        df[column] = pd.to_numeric(df[column], errors='coerce')
        df[column] = df[column].astype(int)
        return df
    else:
        return "Invalid input"


def switch_date_format(df: pd.DataFrame, date_col: str, format_to: str):
    """switch_date_format: changes dates from m/d/y, to d/m/y, or vice versa
        args:
            df: a pandas dataframe with date data.
            date_col: name of column with date info
            format_to: desired end format of string
        returns:
            df: a pandas_df with reformatted date column
    """
    if format_to == '%d/%m/%Y':
        df[date_col] = pd.to_datetime(df[date_col], format='%m/%d/%Y').dt.strftime('%d/%m/%Y')
    elif format_to == '%m/%d/%Y':
        df[date_col] = pd.to_datetime(df[date_col], format='%d/%m/%Y').dt.strftime('%m/%d/%Y')

    else:
        print('not valid format')

    return df


def to_decimal_degrees(coord: str, num_digits: int):
    """to_decimal_degrees: this function is for the conversion of degrees from
       hours, minutes, seconds format to straight decimal degrees.
       args:
            coord: the coordinate string to convert to decimal.
        returns:
            num_coord: the new coordinate converted into numeric decimal format"""

    deg, minutes, seconds, direction = re.split('[°\'"]', coord)

    num_coord = (float(deg) + float(minutes) / 60 + float(seconds) / (60 * 60)) * \
                (-1 if direction in ['W', 'S'] else 1)

    return round(num_coord, num_digits)


def zero_out_barcode(number):
    """changes barcode to specify barcode with leading zeroes, function made for lapply"""
    return str(number).zfill(9)
