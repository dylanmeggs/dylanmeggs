#Tasks
#Customer should be able to display all the books available in the library
#Handle the process when a customer requests to borrow a book
#Update the library collection when the customer returns a book


class Library:

    def __init__(self, listOfBooks):
        self.availableBooks = listOfBooks
    
    def displayAvailableBooks(self):
        print()
        print("Available Books: ")
        for book in self.availableBooks:
            print(book)

    def lendBook(self,requestedBook):
        if requestedBook in self.availableBooks:
            print("You have now borrowed the book")
            self.availableBooks.remove(requestedBook)
        else:
            print("Sorry, the book is not available in our list.")

    def addBook(self, returnedBook):
        self.availableBooks.append(returnedBook)
        print("You have returned the book. Thank you!")

class Customer:
    def requestBook(self):
        print()
        print("Enter the name of a book you would like to borrow: ")
        self.book = input()
        return self.book
        print()

    def returnBook(self):
        print()
        print("Enter the name of the book which you are returning:")
        self.book = input()
        return self.book
        print()

library = Library(['Catcher and the Rye', 'Inciting Joy', 'Of Mice and Men', 'As I Lay Dying'])
customer = Customer()
while True:
    print("Enter 1 to display the available books")
    print("Enter 2 to request for a book")
    print("Enter 3 to return a book")
    print("Enter 4 to exit")

    userChoice = int(input())
    if userChoice is 1:
        library.displayAvailableBooks()
    elif userChoice is 2:
        requestedBook = customer.requestBook()
        library.lendBook(requestedBook)
    elif userChoice is 3:
        returnedBook = customer.returnBook()
        library.addBook(returnedBook)
    elif userChoice is 4:
        quit()