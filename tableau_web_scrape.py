import sys
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from azure.storage.blob import BlockBlobService
from sqlalchemy import create_engine
import pandas as pd
from datetime import datetime, timedelta
import time
import keyring

def get_sf_last_90():
    """logs into snowflake and pulls the max date from the brand_overview table"""
    
    #print('Getting the max date from the ' + table_name + ' table in Snowflake...')
    max_date_sql = "SELECT distinct(date) FROM SFPRDH.PLUTOTV.BRAND_OVERVIEW \
                    WHERE date >= DATEADD(day,-93,GETDATE()) and   date <= getdate();"
    try:
        engine = create_engine(
        'snowflake://{user}:{password}@{account}/{database}/{schema}?warehouse=sn_dist_wh'.format(
        database='SFPRDH',
        schema='plutotv',
        user='etl_management_svc',
        password= keyring.get_password("directory", 
                                       "password"),
        account='scripps.east-us-2.azure'
            )
        )
        connection = engine.connect()
        print('Connected to DB: SFPRDH')
        # execute sql
        result = pd.read_sql(max_date_sql,connection)
        result = result.sort_values(by=['date'])
        sf_dates_list = result['date'].tolist()
        return sf_dates_list
    except Exception as e:
        print('Error: {}'.format(str(e)))
        sys.exit(1)
    finally:
        connection.close()
#sf_dates = get_sf_last_90()       

def get_pluto_tv_date(url,email,password):
    """logs into the plutotv dashboard and gets the max available date"""
    
    print('Logging into the Tableau dashboard to get the max date of available data...')
    PATH = "E:\python\chromedriver.exe"  #change this
    options = Options()
    options.add_experimental_option("prefs", {
        "download.default_directory": r"E:\plutotv_scraper\plutotv_downloads",
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True
        })
    driver = webdriver.Chrome(PATH,options=options)
    wait = WebDriverWait(driver, 120)
    for i in range(0,10):
        while True:
            try:
                driver.get(url)
                time.sleep(10)
                wait.until(EC.presence_of_element_located((By.NAME,"email"))).send_keys(email)
                wait.until(EC.presence_of_element_located((By.ID,'login-submit')) ).click()
                wait.until(EC.presence_of_element_located((By.ID,'password'))).send_keys(password)
                wait.until(EC.presence_of_element_located((By.ID,'login-submit')) ).click()
                time.sleep(30)
                iframe = wait.until(EC.presence_of_element_located((By.XPATH,'//*[@id="viz"]/iframe')))
                driver.switch_to.frame(iframe)
                time.sleep(2)
                date_avail = driver.find_element_by_xpath('/html/body/div[2]/div[2]/div[2]/div[1]/div/div[2]/div[15]/div/div/div/div/div/div/div[3]/span/div[1]/span').text
                date_avail = datetime.strptime(date_avail,'%m/%d/%Y').date()
                #date_avail = datetime.strftime(date_avail,'%Y-%m-%d')
                print('The max date available is ' + str(date_avail) + '.')
                return date_avail
                driver.close()
            except TimeoutError as e:
                print('Error: {}'.format(str(e)))
            except Exception as e:
                print('Error: {}'.format(str(e)))
                sys.exit(1)
                continue
            break
        
def get_pluto_last_90():
    
    pluto_max_date = get_pluto_tv_date(url,email,password)
    pluto_start_date = pluto_max_date - timedelta(90)
    
    delta = pluto_max_date - pluto_start_date
    
    pluto_last_90 = []
    
    for i in range(delta.days + 1):
        day = pluto_start_date + timedelta(days=i)
        pluto_last_90.append(day)
        print(day)
    return pluto_last_90

#pluto_90 = get_pluto_last_90()

def get_days_to_scrape():    
    days_to_scrape = list(set(pluto_last_90) - set(sf_dates))
    days_to_scrape = [date_obj.strftime('%m/%d/%Y') for date_obj in days_to_scrape]
    days_to_scrape = sorted(days_to_scrape, key=lambda date: datetime.strptime(date, "%m/%d/%Y"))
    return days_to_scrape


def get_date_ids():
    PATH = "E:\python\chromedriver.exe"  #change this
    options = Options()
    options.add_experimental_option("prefs", {
        "download.default_directory": r"E:\plutotv_scraper\plutotv_downloads",
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True
        })
    driver = webdriver.Chrome(PATH,options=options)
    wait = WebDriverWait(driver, 120)
    for i in range(0,10):
        while True:
            try:
                driver.get(url)
                time.sleep(10)
                wait.until(EC.presence_of_element_located((By.NAME,"email"))).send_keys(email)
                wait.until(EC.presence_of_element_located((By.ID,'login-submit')) ).click()
                wait.until(EC.presence_of_element_located((By.ID,'password'))).send_keys(password)
                wait.until(EC.presence_of_element_located((By.ID,'login-submit')) ).click()
                time.sleep(30)
                if driver.find_element_by_xpath('/html/body/div[1]/div[3]/div/div/div/div/div[1]/div/div/input'):
                    checkbox_click = wait.until(EC.presence_of_element_located((By.XPATH, '/html/body/div[1]/div[3]/div/div/div/div/div[1]/div/div/input')))
                    checkbox_click.click()
                    time.sleep(1)
                    close_click = wait.until(EC.presence_of_element_located((By.XPATH, '/html/body/div[1]/div[3]/div/div/div/div/div[2]/div/div/button')))
                    close_click.click()
#                    time.sleep(10)
                else:
                    pass
                time.sleep(10)
                iframe = wait.until(EC.presence_of_element_located((By.XPATH,'//*[@id="viz"]/iframe')))
                driver.switch_to.frame(iframe)
                print('Logged in!')
                time.sleep(5)
                date_box_click = wait.until(EC.presence_of_element_located((By.XPATH,'/html/body/div[2]/div[2]/div[2]/div[1]/div/div[2]/div[15]/div/div/div/div/div/div/div[3]/span/div[2]')))
#                date_box_click = wait.until(EC.presence_of_element_located((By.XPATH,'/html/body/div[2]/div[2]/div[2]/div[1]/div/div[2]/div[13]/div/div/div/div/div/div/div[3]/span/div[2]')))
                date_box_click.click()
                time.sleep(2)
                id_elements = wait.until(EC.presence_of_element_located((By.XPATH,'//*[@id="tableau_base_widget_LegacyCategoricalQuickFilter_2_menu"]'))).find_elements(By.CLASS_NAME,'FIItem')
                date_ids = []
                for eachElement in id_elements:
                    individual_ids = eachElement.get_attribute("id")
                    date_ids.append(individual_ids)
                date_elements = wait.until(EC.presence_of_element_located((By.XPATH,'//*[@id="tableau_base_widget_LegacyCategoricalQuickFilter_2_menu"]'))).find_elements(By.CLASS_NAME,'FIText')
                date_names = []
                for eachElement in date_elements:
                    individual_dates = eachElement.get_attribute("title")
                    date_names.append(individual_dates)
                series_ids = []
                for eachElement in id_elements:
                    individual_ids = eachElement.get_attribute("id")
                    series_ids.append(individual_ids)
                time.sleep(5)
                driver.close()
                dates_dict = dict(zip(date_names,date_ids))
                dates_dict.pop('(All)')
                dates_dict = {datetime.strptime(key, "%m/%d/%Y").strftime('%m/%d/%Y'): val for key, val in dates_dict.items()}
            except TimeoutError as e:
                print('Error: {}'.format(str(e)))
            except Exception as e:
                print('Error: {}'.format(str(e)))
                sys.exit(1)
                continue
            break
        return dates_dict


def get_dates():
    sf_dates = get_sf_last_90()
    pluto_last_90 = get_pluto_last_90()
    days_to_scrape = list(set(pluto_last_90) - set(sf_dates))
    days_to_scrape = [date_obj.strftime('%m/%d/%Y') for date_obj in days_to_scrape]
    days_to_scrape = sorted(days_to_scrape, key=lambda date: datetime.strptime(date, "%m/%d/%Y"))

    dates_dict = get_date_ids()
    dates_dict = {datetime.strptime(key, "%m/%d/%Y").strftime('%m/%d/%Y'): val for key, val in dates_dict.items()}
    dates = {x: dates_dict[x] for x in days_to_scrape}
    return dates



def write_to_blob(filename,to_file):
    """connects to azure and submits the file(s) to a blob"""
    
    print('Writing data to blob...')
    storage_account_name = 'saewsdatafactorydevwest2'
    storage_account_key = '056Xy1EeqYuFsQ5Ek7ksn8Vt/RQPSCGIqrQ48TBtSK6TZOoHn7SyDrEC3RzzELsfIel5KvP6pJBK+AStC3oAxQ=='
    block_blob_service = BlockBlobService(account_name=storage_account_name
                                              , account_key=storage_account_key)
    try:
        block_blob_service.create_blob_from_text('sources/plutotv', filename, to_file)
        print('Data has been written to the blob!')
    except Exception as e:
        print('Error: {}'.format(str(e)))
        sys.exit(1)

def write_to_snowflake(stmt_sql):
    """connects to snowflake and sumbits the provided sql statement"""
    
    print('Writing the data to snowflake...')
    engine = create_engine(
        'snowflake://{user}:{password}@{account}/{database}/{schema}?warehouse=it_datateam_wh'.format(
        database='SFPRDH',
        schema='plutotv',
        user='etl_management_svc',
        password= keyring.get_password("directory", 
                                       "password"),
        account='scripps.east-us-2.azure'
            )
        )
    try:
        connection = engine.connect()
        print('connected to snowflake')
        connection.execute(stmt_sql)
        print('success!')
    except Exception as e:
        print('Error: {}'.format(str(e)))
        sys.exit(1)
        
def print_series(dict):
    print('The series to be scraped are:')
    for key, value in dict.items():
        print("{} (id: {})".format(key, value))





def scrape(url,dict1,dict2,email,password,all_check_name,episode_button_xpath,period,csv_list,table_name,date_range):
    """scrapes the provided url for all the necessary data and downloads it or appends it"""
    

    partner_filter_xpath = '/html/body/div[2]/div[2]/div[2]/div[1]/div/div[2]/div[7]/div/div/div/div/div/div/div[3]/span/div[2]'
    apply_xpath = '//*[@id="tableau_base_widget_LegacyCategoricalQuickFilter_0_menu"]/div[3]/button[2]' #apply button in dropdown
    search_apply_xpath = '/html/body/div[7]/div[3]/button[2]'

    PATH = "E:\python\chromedriver.exe"  #change this
    options = Options()
    options.add_experimental_option("prefs", {
      "download.default_directory": r"E:\plutotv_scraper\plutotv_downloads",
      "download.prompt_for_download": False,
      "download.directory_upgrade": True,
      "safebrowsing.enabled": True
    })
    driver = webdriver.Chrome(PATH,options=options)
    
    wait = WebDriverWait(driver, 60) #60 second wait object
    actions = ActionChains(driver) #actions chain object
    
    #run check
    if not any(dates) == True:
        result = 'The available data has not been updated.'
        sys.exit(1)
    else:
        pass
        
    
    # initiate session
    driver.get(url)
    print('Beginning scrape...')
    # login to dashboard
    try:
        wait.until(EC.presence_of_element_located((By.NAME,"email")) ).send_keys(email)
        wait.until(EC.presence_of_element_located((By.ID,'login-submit')) ).click()
        wait.until(EC.presence_of_element_located((By.ID,'password')) ).send_keys(password)
        wait.until(EC.presence_of_element_located((By.ID,'login-submit')) ).click()
    except Exception as e:
        print(str(e))
    pass
    time.sleep(30)
    if driver.find_element_by_xpath('/html/body/div[1]/div[3]/div/div/div/div/div[1]/div/div/input'):
        checkbox_click = wait.until(EC.presence_of_element_located((By.XPATH, '/html/body/div[1]/div[3]/div/div/div/div/div[1]/div/div/input')))
        checkbox_click.click()
        time.sleep(1)
        close_click = wait.until(EC.presence_of_element_located((By.XPATH, '/html/body/div[1]/div[3]/div/div/div/div/div[2]/div/div/button')))
        close_click.click()
        time.sleep(10)
    else:
        pass

    iframe = wait.until(EC.presence_of_element_located((By.XPATH,'//*[@id="viz"]/iframe')))
    driver.switch_to.frame(iframe)
    print('Logged in!')
    time.sleep(10)
    
    for key, value in dates.items():
        date_avail_str = str(key)
        time.sleep(5)
        date_box_click = wait.until(EC.presence_of_element_located((By.XPATH,'/html/body/div[2]/div[2]/div[2]/div[1]/div/div[2]/div[15]/div/div/div/div/div/div/div[3]/span/div[2]')))
        date_box_click.click()
        
        all_box_click = wait.until(EC.presence_of_element_located((By.XPATH, '/html/body/div[7]/div[2]/div[1]/div[2]/input')))
        all_box_click.click()
        all_box_click.click()
        
        text_box_click = wait.until(EC.presence_of_element_located((By.XPATH,'/html/body/div[7]/div[1]/div/textarea'))).send_keys(key)
        value = value.replace('F', 'S', 1)
        
        date_checkbox_click = wait.until(EC.presence_of_element_located((By.NAME, value)))
        date_checkbox_click.click()

        apply_click = wait.until(EC.presence_of_element_located((By.XPATH, search_apply_xpath)))
        apply_click.click()

        driver.refresh()
        iframe = wait.until(EC.presence_of_element_located((By.XPATH,'//*[@id="viz"]/iframe')))
        driver.switch_to.frame(iframe)
        time.sleep(10)
        for key,value in dict1.items():
            print('Scraping all ' + key + ' data...')
            metric_list = [key,date_avail_str]
            while True:
                try:
                    print('Selecting ' + key + ' from the partner drop down menu...')
                    #select the partner/brand and refresh
                    
                    partner_click = wait.until(EC.presence_of_element_located((By.XPATH, partner_filter_xpath)))
                    partner_click.click()
                    time.sleep(1)
                    all_click = wait.until(EC.presence_of_element_located((By.NAME, all_check_name)))
                    all_click.click()
                    time.sleep(1)
                    brand_click = wait.until(EC.presence_of_element_located((By.NAME, value)))
                    brand_click.click()
                    time.sleep(1)
                    apply_click = wait.until(EC.presence_of_element_located((By.XPATH, apply_xpath)))
                    apply_click.click()
                    time.sleep(2)
                    driver.refresh()
                    time.sleep(10)
                    iframe = wait.until(EC.presence_of_element_located((By.XPATH,'//*[@id="viz"]/iframe')))
                    driver.switch_to.frame(iframe)
                    time.sleep(5)
                    partner_value = wait.until(EC.presence_of_element_located((By.XPATH,'/html/body/div[2]/div[2]/div[2]/div[1]/div/div[2]/div[7]/div/div/div/div/div/div/div[3]/span/div[1]/span'))).text
                    print('Scraping the ' + key + ' metric data...')
                    try:
                        for key, value in dict2.items():
                            main_window = driver.window_handles[0]
                            time.sleep(2)
                            metric_box = driver.find_element_by_xpath(value)
                            if metric_box.is_enabled():
                                metric_box.click()
                                hit_enter = actions.send_keys(Keys.RETURN)
                                hit_enter.perform()
                                time.sleep(5)
                                new_window = driver.window_handles[1]
                                time.sleep(1)
                                driver.switch_to.window(new_window)
                                time.sleep(5)
                                try:
                                    metric = driver.find_element_by_xpath('/html/body/div/div/div/div/div[2]/div[2]/div/div/div/div/div[2]/div/div/div/div/div').text
                                    metric = metric.replace(',','')
                                    metric = float(metric)
                                except:
                                    metric = 0
                                    pass
                            else:
                                metric = 0
                            driver.switch_to.window(main_window)
                            time.sleep(1)
                            hit_enter = actions.send_keys(Keys.RETURN)
                            hit_enter.perform()
                            time.sleep(1)
                            iframe = wait.until(EC.presence_of_element_located((By.XPATH,'//*[@id="viz"]/iframe')))
                            driver.switch_to.frame(iframe)
                            time.sleep(2)
                            #create list and append
                            metric_list.append(metric)
                            time.sleep(2)
                        print('Got the metric data!')
                        metric_list_series = pd.Series(metric_list, index = df.columns)
                        df = df.append(metric_list_series,ignore_index=True)
                        time.sleep(10)
                        print('Resetting...')
                        partner_click = wait.until(EC.presence_of_element_located((By.XPATH, partner_filter_xpath)))
                        partner_click.click()
                        time.sleep(1)
                        all_click = wait.until(EC.presence_of_element_located((By.NAME, all_check_name)))
                        all_click.click()
                        time.sleep(1)
                        apply_click = wait.until(EC.presence_of_element_located((By.XPATH, apply_xpath)))
                        apply_click.click()
                        time.sleep(5)
                        driver.refresh()
                        time.sleep(10)
                        iframe = wait.until(EC.presence_of_element_located((By.XPATH,'//*[@id="viz"]/iframe')))
                        driver.switch_to.frame(iframe)
                    except:
                        pass
                except NoSuchElementException as e:
                    print('Error: {}'.format(str(e)))
                    continue
                except TimeoutError as e:
                    print('Error: {}'.format(str(e)))
                    continue
                break





def main():

    url = 'tableau_url' #daily partner dashoard

    # login info for dashboard
    email = 'email'
    password = keyring.get_password('directory', 
                                    'email')

    ########## DAILY DASHBOARD ##########

    # daily brand dictionary
    brand = {'Bounce XL':'FI_federated.0kbsa2z0olkou60zwojes0fwxd2w,none:PARENT_PARTNER_NAME:nk18122786619998846899_7110047036474039373_0',
            'Court TV':'FI_federated.0kbsa2z0olkou60zwojes0fwxd2w,none:PARENT_PARTNER_NAME:nk18122786619998846899_7110047036474039373_1',
            'Newsy':'FI_federated.0kbsa2z0olkou60zwojes0fwxd2w,none:PARENT_PARTNER_NAME:nk18122786619998846899_7110047036474039373_2',
            'Newsy (CSV)':'FI_federated.0kbsa2z0olkou60zwojes0fwxd2w,none:PARENT_PARTNER_NAME:nk18122786619998846899_7110047036474039373_3'}

    # daily metric dictionary
    metrics = {'users_xpath':'//*[@id="view18122786619998846899_7110047036474039373"]/div[1]/div[2]/canvas[2]',
            'sessions_xpath':'//*[@id="view18122786619998846899_616919030795710020"]/div[1]/div[2]/canvas[2]',
            'tvm_xpath':'//*[@id="view18122786619998846899_4491375143712039986"]/div[1]/div[2]/canvas[2]',
            'ampu_xpath':'//*[@id="tabZoneId24"]/div/div/div/div[1]'}


    all_check_name = 'FI_federated.0kbsa2z0olkou60zwojes0fwxd2w,none:PARENT_PARTNER_NAME:nk18122786619998846899_7110047036474039373_(All)' #checkbox for 'all' daily
    episode_button_xpath = '/html/body/div[8]/div/div/div/div/div[2]/div/div[1]/div[2]/div/div/div[1]/div/span' #episode crosstab button daily

    # get dates to run
    dates = get_dates()

    dates = {datetime.strptime(key, "%m/%d/%Y").strftime('%#m/%#d/%Y'): val for key, val in dates.items()}


    date_avail_str = datetime.today().strftime('%Y-%m-%d')
    period = 'Daily'

    csv_list = []
    metric_list = []
    metric_list_series = []
    df = pd.DataFrame(columns = ['brand','date','users','sessions','total_viewing_minutes','avg_min_per_user'])    

    while True:
        try:
            result = ''
            scrape(url,brand,metrics,email,password,all_check_name,episode_button_xpath,period,csv_list,'brand_overview','date')
            e = None

            if result != 'The available data has not been updated.':
                filename = f'brands_{period.lower()}_{date_avail_str}.csv'
                df.to_csv(f'E:\\plutotv_scraper\\plutotv_data\\' + filename +'',index=False)
                df = pd.read_csv(f'E:\\plutotv_scraper\\plutotv_data\\brands_{period.lower()}_{date_avail_str}.csv')
                to_file = df.to_csv(index=False)

                write_to_blob(filename,to_file)

                # copy brand df from stage into snowflake    
                stmt_sql = "copy into SFPRDH.PLUTOTV.BRAND_OVERVIEW from (select $1, $2, $3, $4, $5, $6, current_timestamp(),metadata$filename from @plutotv_stg) \
                file_format = (format_name = SFPRDH.PLUTOTV.BRAND_CSV), pattern = '.*brands_" + period.lower() + "_" + date_avail_str + ".csv';"

                write_to_snowflake(stmt_sql)
                            
            else:
                print('No data needed to be written.')
        except NoSuchElementException as e:
            print('Error: {}'.format(str(e)))
            continue
        except TimeoutError as e:
            print('Error: {}'.format(str(e)))
            continue
        break

if __name__ == "__main__":
    main()

