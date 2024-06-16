# Utilities functions
import tkinter as tk
import pandas as pd
from tkinter import messagebox
from src.utils.alerts import AlertType


def show_message(alert_type=AlertType.FAILED):
    """
    Function that shows an Error message box when a query does not execute properly
    Or an informational message box if the operation completed successfully
    :param alert_type: The alert type (success or error)
    :type alert_type: AlertType
    """
    # Create a Tkinter root window
    root = tk.Tk()
    root.withdraw()

    # Show an error message box with an OK button
    if alert_type == AlertType.FAILED:
        messagebox.showerror("ERROR", "There has been an error and the operation did not complete.\n"
                                      "Please check the log for more detail.")
    # Show a message box with an OK button
    elif alert_type == AlertType.SUCCESS:
        messagebox.showinfo("SCRIPT OPERATION SUCCESSFUL",
                            "Program completed successfully.")

    # Close the Tkinter root window
    root.destroy()


def datetime_from_py_to_sql(py_datetime):
    """
    Function that converts python datetime format to SQL datetime format
    :param py_datetime: A datetime object
    :type py_datetime: datetime.datetime
    :return: A String resembling SQL datetime type
    :rtype: str
    """
    return py_datetime.__str__()[:23]


def remove_dollar_sign(value: str, currency='$'):
    """
    Function takes a string value in currency format and removes the currency symbol. It converts the currency value
    to float
    :param value: The currency value in string type. E.g.: $1.00
    :param currency: The currency symbol
    :return: The currency value in float format. E.g.: $1.00 -> 1.00
    :rtype: float
    """
    new_value = value.strip()
    new_value = new_value.replace(',', '')
    return float(new_value.replace(currency, ''))


def iterable_splitter(iterable_object, column_name='SerialNumber', chunk_size=None):
    """
    Function splits an iterable object such as a list or a dataframe (by column name)
    into four partitions (15%-35%-20%-30%); or into various partitions in sizes of chunk_size (if defined)
    :param iterable_object: The list or dataframe to be split into four
    :param column_name: The column name to use for the splitting. Used only if the iterable object is
    a dataframe and if the split is of fixed sizes (15%-35%-20%-30%)
    :param chunk_size: If defined, it is used to partition a dataframe in sizes of chunk_size
    :return: The iterable object partitioned
    """
    if type(iterable_object) is pd.DataFrame and chunk_size:  # Splitter for Dataframes by chunk size
        num_chunks = iterable_object.shape[0] // chunk_size
        if num_chunks > 0:
            chunked_dfs = [iterable_object.iloc[i * chunk_size: (i + 1) * chunk_size, :].copy()
                           for i in range(num_chunks)]
            remaining_df = iterable_object.iloc[(num_chunks * chunk_size):, :].copy()

            if remaining_df.shape[0] > 0:  # Append remaining dataframe (smaller than chunk_size) if any
                chunked_dfs.append(remaining_df)
            return chunked_dfs
        elif 0 < iterable_object.shape[0] < chunk_size:
            return [iterable_object.copy()]
        else:
            return []

    elif type(iterable_object) is pd.DataFrame:  # Splitter for Dataframes by percentage
        unique_category_values = list(set(iterable_object[column_name]))
        partition_sizes = (int(len(unique_category_values) * 0.15), int(len(unique_category_values) * 0.50),
                           int(len(unique_category_values) * 0.70))
        division_1 = unique_category_values[:partition_sizes[0]]
        division_2 = unique_category_values[partition_sizes[0]: partition_sizes[1]]
        division_3 = unique_category_values[partition_sizes[1]: partition_sizes[2]]
        division_4 = unique_category_values[partition_sizes[2]:]
        new_df_1 = iterable_object[iterable_object[column_name].isin(division_1)]
        new_df_2 = iterable_object[iterable_object[column_name].isin(division_2)]
        new_df_3 = iterable_object[iterable_object[column_name].isin(division_3)]
        new_df_4 = iterable_object[iterable_object[column_name].isin(division_4)]

        return new_df_1, new_df_2, new_df_3, new_df_4, division_1, division_2, division_3, division_4

    elif type(iterable_object) is list:  # Splitter for lists
        partition_sizes = (int(len(iterable_object) * 0.15), int(len(iterable_object) * 0.50),
                           int(len(iterable_object) * 0.70))
        new_list_1 = iterable_object[:partition_sizes[0]]
        new_list_2 = iterable_object[partition_sizes[0]:partition_sizes[1]]
        new_list_3 = iterable_object[partition_sizes[1]:partition_sizes[2]]
        new_list_4 = iterable_object[partition_sizes[2]:]

        return new_list_1, new_list_2, new_list_3, new_list_4
