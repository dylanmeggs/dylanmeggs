
from selenium import webdriver
from bs4 import BeautifulSoup
import pandas as pd

driver = webdriver.Firefox(executable_path=r'C:\Users\username\Desktop\Python\Gecko\geckodriver.exe')
driver.get("https://dev.to")
driver.find_element_by_id("nav-search").send_keys("Selenium")
