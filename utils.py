import pickle

def save_obj(obj, name):
    """
    This function save an object as a pickle.
    :param obj: object to save
    :param name: name of the pickle file.
    :return: -
    """
    with open(name + '.pkl', 'wb') as f:
        pickle.dump(obj, f, pickle.HIGHEST_PROTOCOL)
    # with open(name + ".json", "w") as f:
    #     json.dump(obj, f)


def load_obj(name):
    """
    This function will load a pickle file
    :param name: name of the pickle file
    :return: loaded pickle file
    """
    with open(name + '.pkl', 'rb') as f:
        return pickle.load(f)
    # with open(name + ".json", "r") as f:
    #     return json.load(f)


def open_file(name):
    """
    This function will load the pickle file to main memory
    and return the handle for the file.
    """
    return open(f'{name}.pkl', 'rb')


def append(obj, name):
    """
    This function will append an object to a file.
    If the file does not exist it will create it first.
    """
    with open(name + ".pkl", 'ab') as fp:
        pickle.dump(obj, fp, pickle.HIGHEST_PROTOCOL)


def close_file(file_handle):
    """
    This function will close the file file pointed by the file_handle
    """
    file_handle.close()

def get_next(file_handle):
    """
    The function will return the next object stored in the pickle file.
    If the last object has already been read, a None will return. ( You will need to open the file again)
    file_handle will be the file_handle object from the open_file function.
    """
    try:
        return pickle.load(file_handle)
    except EOFError:
        return None

def load_inverted_index():
    """
    The function will get the path for the inverted index
    and will return the inverted idx object as a dictionary.
    """
    return load_obj("inverted_idx")
