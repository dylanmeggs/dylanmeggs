class Employee:
    def employeeDetails(self):
        self.name = "Ben"

    @staticmethod
    def welcomeMessage():
        print("Welcome to our organization!")

#Create an object for this class and envoke the method employeedetails
employee = Employee()
employee.employeeDetails()
print(employee.name)
employee.welcomeMessage()



#Static methods do not take the default self parameter.
#Use decorator. Decorators are functions that take another function and extend their functionality.
#These start with @
#Static method is a decorator which takes the welcome message and ignores the binding of the object
#This allows it to execute without any problem
#Why would we want this? Idk why there are different types
