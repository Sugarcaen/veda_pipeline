def clean_keys(dict_to_clean):
    return {k.lower().replace(" ", "_"): v for k, v in dict_to_clean.items()}