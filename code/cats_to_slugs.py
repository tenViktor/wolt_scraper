def make_cats_slugs(filepath:str) -> list:    
    """
    Converts categories from text file urls into valid wolt category slugs
    :param (str) filepath: path of txt file
    """
    
    slug_list = list()
    
    with open(filepath, mode="r") as file:
        cat_list = list(file)
        for slug in cat_list:
            slug = slug.split("/")
            slug_list.append(slug[-1].removesuffix("n").strip())
    return slug_list

if __name__ == "__main__":
    print(make_cats_slugs("cats.txt"))