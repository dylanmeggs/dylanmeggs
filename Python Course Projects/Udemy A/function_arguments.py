def hello_func(firstName = "User"):  #added user later as a default value
    print("Hello " + firstName)
    
hello_func("Dylan") # result is Hello Dylan

def addNums(arg1, arg2):
    print(arg1 + arg2)
    addNums(3,4) # result is 7

def hello_func2(firstName, middle, last):
    print("Hello " + firstName + " " + middle + " " + last)
    hello_func2(firstName = "Dylan", last = "Meggs", middle = "Wade") 
# naming the arguments makes it more readable and allows you to submit out of order to get the right order in final result
# hello_func()  won't work if you've added an argument. However, if you set a default value in argument, it will work. It prints default
hello_func()
