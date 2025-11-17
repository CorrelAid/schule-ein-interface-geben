
from lib.models import DownloadCategoryNode
from anytree import RenderTree, Resolver
from anytree.resolver import ChildResolverError
from anytree.exporter import JsonExporter
from anytree.importer import JsonImporter
import polars as pl

def build_category_tree(ul, parent_node=None):
    if parent_node is None:
        root_node = DownloadCategoryNode("root", data_id="0", data_level="-1")
        build_category_tree(ul, root_node)
        return root_node.children[0] if root_node.children else None
    else:
        for li in ul.find_all("li", recursive=False):
            data_id = li.get("data-id", "")
            data_level = li.get("data-level", "")
            data_parent_id = li.get("data-parent-id", "")
            title_span = li.find("span", class_="title")
            name = title_span.get_text(strip=True) if title_span else "Untitled"

            node = DownloadCategoryNode(
                name=name,
                data_id=data_id,
                data_level=data_level,
                data_parent_id=data_parent_id,
                parent=parent_node 
            )

            nested_ul = li.find("ol", class_="dd-list") or li.find("ul", class_="dd-list")

            if nested_ul:
                build_category_tree(nested_ul, node)

        return None
    
def find_node_by_id(root_node, target_id):
    if root_node.data_id == target_id:
        return root_node
    for child in root_node.children:
        found = find_node_by_id(child, target_id)
        if found:
            return found
    return None

def get_node_lst(root_node):
    node_ids = []

    for pre, _, node in RenderTree(root_node):
        node_ids.append(node.data_id)

    return node_ids


def export_tree_to_json(root_node,file_path):
    exporter = JsonExporter(indent=2, sort_keys=True)
    jason = exporter.export(root_node)
    with open(file_path, "w") as f:
        f.write(jason)

def import_tree_from_json(file_path):
    importer = JsonImporter()
    with open(file_path, "r") as f:
        root_node = importer.import_(f.read())
    return root_node


def get_all_category_ids(cat_id, root_node, resolver):
    """Get this category ID and all descendant category IDs from the tree."""
    if cat_id is None:
        return []

    category_ids = [cat_id]

    try:
        node = resolver.get(root_node, str(cat_id))
        for child in node.children:
            child_id = int(child.data_id)
            category_ids.extend(get_all_category_ids(child_id, root_node, resolver))
    except ChildResolverError:
        pass

    return category_ids


def add_associated_downloads(posts_df, downloads_df, root_node):
    """Add a column with associated downloads for each post based on category tree traversal."""
    # Prepare data
    dl_dicts = downloads_df[["data_id", "data_category_id"]].to_dicts()
    post_dicts = posts_df[["id", "download_chapter_dedicated"]].to_dicts()

    # Create resolver for tree traversal
    resolver = Resolver('data_id')

    # Build mapping of post_id -> list of associated download IDs
    post_downloads_map = {}
    for post in post_dicts:
        post_id = post["id"]
        cat_id = post["download_chapter_dedicated"]

        # Get all category IDs (post category + all descendants)
        all_cat_ids = get_all_category_ids(cat_id, root_node, resolver)

        # Find downloads whose category is in this tree
        matched = [
            dl['data_id'] for dl in dl_dicts
            if dl['data_category_id'] in all_cat_ids
        ]

        post_downloads_map[post_id] = matched if matched else None

    # Add the new column to the dataframe
    posts_df = posts_df.with_columns(
        pl.col("id").replace(post_downloads_map, default=None).alias("associated_downloads")
    )

    return posts_df


def add_associated_posts(downloads_df, posts_df, root_node):
    """Add a column with associated posts for each download based on category tree traversal.

    This is the inverse of add_associated_downloads - for each download, find all posts
    whose category tree includes this download's category.
    """
    # Prepare data
    dl_dicts = downloads_df[["data_id", "data_category_id"]].to_dicts()
    post_dicts = posts_df[["id", "download_chapter_dedicated"]].to_dicts()

    # Create resolver for tree traversal
    resolver = Resolver('data_id')

    # Build mapping of download_id -> list of associated post IDs
    download_posts_map = {}

    for dl in dl_dicts:
        download_id = dl["data_id"]
        download_cat_id = dl["data_category_id"]

        # Find all posts whose category tree includes this download's category
        associated_post_ids = []
        for post in post_dicts:
            post_id = post["id"]
            post_cat_id = post["download_chapter_dedicated"]

            # Get all category IDs for this post's tree
            all_cat_ids = get_all_category_ids(post_cat_id, root_node, resolver)

            # If the download's category is in this post's tree, add the post
            if download_cat_id in all_cat_ids:
                associated_post_ids.append(post_id)

        download_posts_map[download_id] = associated_post_ids if associated_post_ids else None

    # Add the new column to the dataframe
    downloads_df = downloads_df.with_columns(
        pl.col("data_id").replace(download_posts_map, default=None).alias("associated_posts")
    )

    return downloads_df