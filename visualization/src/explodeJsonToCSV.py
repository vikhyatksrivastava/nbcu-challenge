import json
import csv
# Part 0 - Data preparation.
def explodejson(jsondata, parent='', delimitter='_'):
    element = {}
    for key, value in jsondata.items():
        newkey = parent + delimitter + key if parent else key
        if isinstance(value, dict):
            element.update(explodejson(value, newkey, delimitter))
        else:
            element[newkey] = value
    return element

def jsontocsv(jsonfile, csvfile):
    with open(jsonfile, 'r') as file:
        jsondata = json.load(file)

    csvdata = []
    for record in jsondata:
        flatentry = explodejson(record)
        csvdata.append(flatentry)

    if csvdata:
        keys = csvdata[0].keys()
        with open(csvfile, 'w', newline='') as file:
            csvwriter = csv.DictWriter(file, fieldnames=keys)
            csvwriter.writeheader()
            csvwriter.writerows(csvdata)
        print("Json file exploded to csv file successfully!")
    else:
        print("No data in JSON!")

def main():
    jsonfile = '../peacock-de-eval.tar/peacock-de-eval/ratings.tar/ratings/data/messages/messages.json'
    csvfile = '../peacock-de-eval.tar/output/messages.csv'
    jsontocsv(jsonfile, csvfile)

if __name__ == '__main__':
    main()




