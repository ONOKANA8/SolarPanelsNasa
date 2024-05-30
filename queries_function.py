# concatenation retrieve data, we need to znter directory where data are
def concat_function():
    """ Use to concat any csv tables located in a specific repository to create only one table
        You can read the table as dataframe directly by creating a dataframe object
    """
    import os
    import pandas as pd
    #from datetime import datetime
    
    #directory_path = input("Which csv table do you want to transform? Provide their directory path : ")
    directory_path = "datas"
    file_list = os.listdir(directory_path)
    dataframe = pd.DataFrame([])
    for element in file_list:
        if element.startswith("france_data"):
            path_to_file = os.path.join(directory_path, element)
            path_to_file.replace("\\", "/")
            data = pd.read_csv(path_to_file)
            dataframe = pd.concat([dataframe, data], ignore_index=True)
    
    return dataframe