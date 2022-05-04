import json
import time

al = []
authors_present = []
ind = 0
with open("books.json", "r") as fopen, open("new_books.json", "a") as fopen1:
    while fopen:
        if ind == 1300000:
            break
        data = json.loads(fopen.readline())
        if "authors" not in data:
            fopen.__next__()
            continue
        if not data["publication_date"]:
            fopen.__next__()
            continue
        for author in data["authors"]:
            authors_present.append(author["id"])
        if ind % 100000 == 0:
            print(ind, "\n", data["title"])
        if len(data["shelves"]) > 15:
            data["shelves"] = data["shelves"][:15]
        data["publication_date"] = int(data["publication_date"].split('-')[0])
        del data["work_id"]
        del data["isbn13"]
        del data["asin"]
        del data["rating_dist"]
        del data["ratings_count"]
        del data["original_publication_date"]
        del data["edition_information"]
        del data["image_url"]
        del data["series_id"]
        del data["series_name"]
        del data["series_position"]
        fopen1.write(json.dumps(data))
        fopen1.write("\n")
        ind += 1
        fopen.__next__()

print("NEW BOOKS JSON FINISHED")

ind = 0
with open("authors.json", "r") as fopen, open("new_authors.json", "a") as fopen1:
    while fopen:
        try:
            if not ind % 10000:
                print(ind)
            if type(fopen.readline()) != str:
                fopen.__next__()
                continue
            data = json.loads(fopen.readline())
            if data["id"] in authors_present:
                del data["ratings_count"]
                del data["text_reviews_count"]
                del data["work_ids"]
                del data["image_url"]
                del data["about"]
                fopen1.write(json.dumps(data))
                fopen1.write("\n")
            fopen.__next__()
            ind += 1
        except Exception:
            fopen.__next__()
            ind += 1
            continue

print("NEW AUTHORS JSON FINISHED")
