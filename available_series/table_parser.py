from bs4 import BeautifulSoup
import csv
import pandas as pd


def table_to_csv(html, filename):

    soup = BeautifulSoup(html.read(), "lxml")
    table = soup.find("table")

    output_rows = []
    for table_row in table.findAll('tr'):
        columns = table_row.findAll('th') + table_row.findAll('td')
        output_row = []
        for column in columns:
            output_row.append(column.text)
        output_rows.append(output_row)
    
    output_rows = [l for l in output_rows if l!=[]]
        
    df = pd.DataFrame(output_rows[1:], columns=output_rows[0])
    df["Series ID"] = df["Series ID"].str.replace("\n", "")

    df.to_csv(filename, index=False, quoting=csv.QUOTE_NONNUMERIC)

    return df