def csv_to_lod(filepath):
    dev_list = [] 
    with open(filepath, mode='r') as file:
        csv_reader = csv.DictReader(file)
        for row in csv_reader:
            row_dict = dict(row)  
            dev_list.append(row_dict)
    return dev_list
