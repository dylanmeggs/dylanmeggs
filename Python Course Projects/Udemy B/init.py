class Employee:

    def __init__(self):
        self.name = "Mark"

    def displayEmployeeDetails(self):
        print(self.name)

employee = Employee()
#Object/Variable = Class
employee.displayEmployeeDetails()
#AttributeError: 'Employee' object has no attribute 'name'
#It failed despite us using Self parameter. 
#This is because we called the second method without initializing the first method.

#Initially we had a def enterEmployeeDetails with name in it.. just regular function
#You need to make sure that you're initializing all of tyour attributes with an init method. 
#This fully initializes your object to make sure you can access any method

class Employee:
    def __init__(self,name):
        self.name = "Mark"

    def displayEmployeeDetails(self):
        print(self.name)

employee = Employee()
#Object/Variable = Class
employee.displayEmployeeDetails()
employeeTwo = Employee("Matthew")
employeeTwo.displayEmployeeDetails()

#Adding a second parameter allows you to add/edit attributes when using init