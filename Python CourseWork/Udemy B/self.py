#Create a method for class without self parameter then learn why we need it
class Employee:
    def employeeDetails():
        pass #do nothing

employee = Employee()
employee.employeeDetails()
#TypeError: Employee.employeeDetails() takes 0 positional arguments but 1 was given
#Python interpreter reads it as Employee.employeeDetails(employee)
#It puts the object in your method despite you having given the attribute no default parameter to use...

class Employee:
    def employeeDetails(self):
        self.name = 'Matthew'
        print("Name = ", self.name)
        age = 30
        print("Age = ",age)
    def printEmployeeDetails(self):
        print("Printing in another method")
        print("Name: ",self.name)
        print("Age: ", age)
    #Age in the second method will fail unless you update to self.age in both methods
    #Their lifecycle ends when the first method finishes running
    #Addint self somehow keeps it alive...

employee = Employee()
employee.employeeDetails()
employee.printEmployeeDetails()

