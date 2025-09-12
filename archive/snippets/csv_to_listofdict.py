def write_dicts_to_csv(dict_list, file_name):
    if not dict_list:
        print("The list of dictionaries is empty.")
        return
    headers = dict_list[0].keys()
    try:
        with open(file_name, 'w', newline='') as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=headers)
            writer.writeheader()
            writer.writerows(dict_list)
        print(f"Data successfully written to {file_name}")
    except IOError:
        print("I/O error occurred while writing to the file.")