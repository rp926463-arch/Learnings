import requests
from bs4 import BeautifulSoup

hive_docs_url = "https://cwiki.apache.org/confluence/display/Hive/LanguageManual+UDF"

response = requests.get(hive_docs_url)

if response.status_code == 200:
    soup = BeautifulSoup(response.content, "html.parser")

    h2_element = soup.find("h2", text="Built-in Functions")
    
    h3_element = soup.find("h3", text="Usage Examples")
    
    div_tags_between_h2_and_h3 = []
    current_element = h2_element.find_next_sibling()

    while current_element and current_element != h3_element:
        if current_element.name == "div" and "table-wrap" in current_element.get("class", []):
            div_tags_between_h2_and_h3.append(current_element)
        current_element = current_element.find_next_sibling()

    target_elements = div_tags_between_h2_and_h3
    
    #target_elements = h2_element.find_all_next("div", class_="table-wrap")

    #target_elements = soup.find_all("div", class_="table-wrap")
    func_array = []
    
    for target_ele in target_elements:
        table_tags = target_ele.find("table")
        tbody_tags = table_tags.find_all("tbody")
        for tbody_tag in tbody_tags:
            tr_elements = tbody_tag.find_all("tr")
            
            print("\n\n")
            print(len(tr_elements))
            for tr in tr_elements:
                td_tags = tr.find_all("td")
                if td_tags and len(td_tags) >= 2:
                    second_td_content = td_tags[1].text.strip()
                    func_name = second_td_content.split('(')[0]
                    print(func_name)
                    func_array.append(func_name)
                    
    print(func_array)