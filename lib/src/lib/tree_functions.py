
from lib.models import DownloadCategoryNode
from anytree import RenderTree
from anytree.exporter import JsonExporter
from anytree.importer import JsonImporter

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