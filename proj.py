from pymongo import MongoClient

HOST_IP = "44.202.48.208"
HOST_PORT = "27017"

author_id = None
book_id = None

def readRecord(books_collection, field_name, value):
    books = books_collection.find({field_name: value})[:10]
    for book in books:
        print(f'Book -> {book["title"]}, Author -> {book["author_name"]}, Year -> {book["publication_date"]}, Rating -> {book["average_rating"]}')


def getRating(books_collection, title):
    rating = books_collection.find({"title", title})[0]["average_rating"]
    print(f'Rating is {rating}. The book is {"Good" if rating > 4 else ("Average" if rating > 3 else "Bad")}')


def findLocation(books_collection, title):
    shelves = books_collection.find({"title": title})[0]["shelves"]
    shelves = [shelf["name"] for shelf in shelves]
    print(f'The book is in the following shelves {shelves}')


def findLessThan(books_collection, field_name, value):
    books = books_collection.find({field_name: {"$lt": value}})[:10]
    for book in books:
        print(f'Book -> {book["title"]}, Author -> {book["author_name"]}, Year -> {book["publication_date"]}, Rating -> {book["average_rating"]}')


def findGreaterThan(books_collection, field_name, value):
    books = books_collection.find({field_name: {"$gte": value}})[:10]
    for book in books:
        print(f'Book -> {book["title"]}, Author -> {book["author_name"]}, Year -> {book["publication_date"]}, Rating -> {book["average_rating"]}')


def insertBook(authors_collection, books_collection, title, author_name, isbn, language, average_rating, reviews, publication_date, format, publisher, description, num_pages):
    global book_id, author_id
    author = authors_collection.find({"name": author_name})
    author = author[0] if author else None
    if author:
        books_collection.insert_one({"id": book_id, "title": title, "author_name": author_name, "author_id": author["id"], "isbn": isbn, "language": language, "average_rating": average_rating, "reviews": reviews, "publication_date": publication_date, "format": format, "publisher": publisher, "description": description, "num_pages": num_pages})
    else:
        authors_collection.insert_one({"id": author_id, "name": author_name, "book_ids": [book_id]})
        books_collection.insert_one({"id": book_id, "title": title, "author_name": author_name, "author_id": author_id, "isbn": isbn, "language": language, "average_rating": average_rating, "reviews": reviews, "publication_date": publication_date, "format": format, "publisher": publisher, "description": description, "num_pages": num_pages})
        author_id += 1
    book_id += 1


def insertAuthor(authors_collection, name, average_rating, fans_count):
    global author_id
    authors_collection.insert_one({"id": author_id, "name": name, "average_rating": average_rating, "book_ids": [], "fans_count": fans_count})
    author_id += 1
    print("Record inserted")


def updateFormat(books_collection, id, format):
    books_collection.update_one({"id": id}, {"$set":{"format": format}})


def updateDescription(books_collection, description, id):
    books_collection.update_one({"id": id}, {"$set": {"description": description}})


def deleteRecord(authors_collection, books_collection, id):
    results = books_collection.delete_one({"id": id})
    authors_collection.update_many({}, {"$pull": {"book_ids": id}})
    print("Completed delete")


def deleteAuthor(authors_collection, books_collection, name):
    authors_collection._delete({"name": name})
    books_collection.delete_many({"author_name": name})
    print(f"Deleted author - {name}")


def deleteTitle(authors_collection, books_collection, title):
    book_id = None
    for d in books_collection.find({"title": title}).limit(1):
        book_id = d["id"]
    books_collection.delete_one({"title": title})
    authors_collection.update_many({}, {"$pull": {"book_ids": book_id}})
    print(f"Deleted title - {title}")


def main():
    flag = True
    operation = None

    uri = "mongodb://"+HOST_IP+":"+HOST_PORT

    client = MongoClient(uri)
    db = client["project"]
    authors_collection = db["authors"]
    books_collection = db["books"]

    while flag:    
        print("1. Query by book name\n 2. Query by author name\n3. Query based on format\n4. Query based on the publisher\n5. Query the rating for a particular title\n6. Query the shelves for a particular title\n7. Find books having less than a certain number of pages\n8. Find books published after a certain year\n9. Find books with an average rating above a certain number\n10. Insert a data entry\n11. Insert an author\n12. Update the Format for a particular title\n13. Update the description for a particular title\n14. Delete an author, delete all associated books with it\n15. Delete a book and remove that book id from all the authors\nEnter any other number to exit")
        operation = int(input())
        match operation:
            case 1:
                print("Enter book title -")
                book_title = input()
                readRecord(books_collection, "book_title", book_title)
            case 2:
                print("Enter an author name -")
                author_name = input()
                readRecord(books_collection, "author_name", author_name)
            case 3:
                print("Enter format to query -")
                format = input()
                readRecord(books_collection, "format", format)
            case 4:
                print("Enter publisher name -")
                publisher = input()
                readRecord(books_collection, "publisher", publisher)
            case 5:
                print("Enter the book title for which you want to know the rating -")
                book_title = input()
                getRating(books_collection, book_title)
            case 6:
                print("Enter the book title for which you want to know the location -")
                book_title = input()
                findLocation(books_collection, book_title)
            case 7:
                print("Enter the number of pages you want books to be less than -")
                num_pages = int(input())
                findLessThan(books_collection, "num_pages", num_pages)
            case 8:
                print("Enter the year you want books to be after -")
                year = int(input())
                findGreaterThan(books_collection, "publication_date", year)
            case 9:
                print("Enter the rating you want books to be greater than")
                rating = int(input())
                findGreaterThan(books_collection, "rating", rating)
            case 10:
                print("Enter title")
                title = input()
                print("Enter author name")
                author_name = input()
                print("Enter ISBN")
                isbn = input()
                print("Enter the language the book is written in")
                language = input()
                print("Enter the average rating the book has")
                average_rating = int(input())
                print("Enter the number of reviews the book has")
                reviews = int(input())
                print("The publication year")
                publication_date = input()
                print("Enter the format the book is in")
                format = input()
                print("Enter the name of the publisher")
                publisher = input()
                print("Enter the description of hte book")
                description = input()
                print("Enter the number of pages the book has")
                num_pages = int(input())
                insertBook(authors_collection, books_collection, title, author_name, isbn, language, average_rating, reviews, publication_date, format, publisher, description, num_pages)
            case 11:
                print("Enter the author name ")
                name = input()
                print("Enter the average rating of the author ")
                average_rating = int(input())
                print("Enter the count of fans ")
                fans_count = int(input())
                insertAuthor(authors_collection, name, average_rating, fans_count)
            case 12:
                print("Enter the ID for which you want to update the format ")
                id = int(input())
                print("Enter the new format")
                format = input()
                updateFormat(books_collection, id, format)
            case 13:
                print("Enter ID for a book you want to edit ")
                id = int(input())
                print("Enter the description ")
                description = input()
                updateDescription(books_collection, description, id)
            case 14:
                print("Enter name of author you want to delete ")
                name = input()
                deleteAuthor(authors_collection, books_collection, name)
            case 15:
                print("Enter the title to delete")
                title = input()
                deleteTitle(authors_collection, books_collection, title)
            case _:
                flag = False
    print("Operations Completed")



if __name__ == "__main__":
    main()






