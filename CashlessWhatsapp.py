import mysql.connector
from datetime import datetime
import schedule
import time
import requests
# import json
import mysql.connector
import requests
import json
import time
import asyncio
def get_whatsapp_business_api_setting(client_id, location_id):
    conn_epion = mysql.connector.connect(user='root', password='rational', host='localhost', database='cashlessai')
    cursor = conn_epion.cursor(dictionary=True)
    whatsapp_data = {}
    try:
        # check if client has purchased whatsapp api and needs to send opd no near mesaage on whatsapp
        query = "SELECT `Key`, `Value` FROM msgwhatsappbusinessapi WHERE Activate = 1 AND ClientID = %s AND LocationID = %s"
        cursor.execute(query, (client_id, location_id))
        dt_whatsapp = cursor.fetchall()
        # print("dt_whatsapp:", dt_whatsapp)
        if dt_whatsapp:  # check if client has whatsapp related data
            # setting values from whatsapp api table
            for row in dt_whatsapp:
                key = row['Key']
                value = row['Value']

                if key == 'WhatsappBusinessAPIKey':
                    APIKey = value
                elif key == 'WhatsappBusinessAPI':
                    WhatsappBusinessAPI = value
                elif key == 'UserName':
                    UserName = value
                elif key == 'APIUrl':
                    APIUrl = value
                elif key == 'Near_TokenNo_CampaignName':
                    Near_Token_CampaignName = value
                elif key == 'OPD_Started_CampaignName':
                    OPD_Started_CampaignName = value
                elif key == 'OPD_Cancelled_CampaignName':
                    OPD_Cancelled_CampaignName = value
                elif key == 'Cashless_Admission_Salus_CampaignName':
                    Cashless_Admission_Salus_CampaignName = value
                elif key == 'Cashless_Admission_Salus_ContactNo':
                    Cashless_Admission_Salus_ContactNo = value

            WhatsBusinessAPIString = f"{APIKey}#{WhatsappBusinessAPI}#{UserName}#{APIUrl}#{Near_Token_CampaignName}#{OPD_Started_CampaignName}#{OPD_Cancelled_CampaignName}#{Cashless_Admission_Salus_CampaignName}#{Cashless_Admission_Salus_ContactNo}"
            # print(WhatsBusinessAPIString)
            return WhatsBusinessAPIString
    except mysql.connector.Error as err:
        print("Error: {}".format(err))

    finally:
        cursor.close()
        conn_epion.close()

    return whatsapp_data

client_id=2
location_id=2

WhatsBusinessAPIStringCall=get_whatsapp_business_api_setting(client_id, location_id)





def save_data():
    # Establish connection to the destination database
    dest_db_connection = mysql.connector.connect(
        user="root", password="rational", host="194.233.80.131", database="cashlessai"
    )

    # Check if the connection to the source database is successful
    if dest_db_connection.is_connected():
        print("Connection to destination database created")
    else:
        print("Connection to destination database failed")
        return  # Return if connection failed

    # Establish connection to MySQL server
    conn = mysql.connector.connect(
        host="localhost",
        user="root",
        password="rational",
        database="epion"
    )

    # Create a cursor object to execute SQL queries
    cursor = conn.cursor()
    cursor1 = dest_db_connection.cursor()

    cutoff_date = datetime(2024, 3, 3).strftime('%Y-%m-%d')

    # Define the select query
    select_query = """
        SELECT 
            adm.PatientID, adm.ADMNo, 
            adm.ClientID, adm.LocationID, adm.DepartmentName, adm.BedName, 
            adm.ADMDateTime, adm.AdmittingDocName, adm.MLCNotes, adm.Status, 
            adm.IsDischarge, adm.Activate, 
            reg.PatientName, reg.Address, reg.City, reg.GSM1, reg.ADHARNo, 
            reg.PANNo, reg.DOB, reg.AgeYear, reg.AgeMonth, reg.AgeDays, 
            reg.Gender, 
            ClaimFormNo, PolicyDetails, PolicyStartDate, PolicyEndDate, 
            WardName, SponsorName, SponsorTypeName, 
            ClaimFormNo, CoPayment, StaffNo, MembershipID 
        FROM 
            trnregistration reg 
        INNER JOIN 
            trnadmission adm ON reg.PatientID = adm.PatientID 
        INNER JOIN 
            trnsponsor ON trnsponsor.OpIpID = adm.AdmissionID 
                       AND trnsponsor.OpIpFlag = 0 
                       AND adm.Activate = 1 

        WHERE 
            adm.ADMDateTime >= '2024-03-01 13:03:51'
            AND SponsorName <> 'Cash'
    """
    # Execute the select query
    cursor.execute(select_query)

    # Fetch all the rows
    rows = cursor.fetchall()

    # Print the rows and check if values are present in test.trnadmission table's ThirdPartyAdmissionID column
    for row in rows:
        # Check if the value exists in test.trnadmission table's ThirdPartyAdmissionID column
        tAdmissionID = row[0]
        check_query = "SELECT AdmissionID FROM trnadmission WHERE ThirdPartyAdmissionID = '" + str(tAdmissionID) + "' AND Activate = 1 "
        cursor1.execute(check_query)
        result = cursor1.fetchall()
        if result:
            print("Value exists in test.trnadmission table's ThirdPartyAdmissionID column")
            continue
        else:
            # Define the insert query
            insert_query = """
                INSERT INTO trnadmission 
                (UHID, ADMNo, ADMDateTime, PatientName, Gender, GSM1, DOB, AgeYear, AgeMonth, AgeDay, Address, City, AdmittingDocName, Department, WardName, RoomName, BedName, SponsorType, SponsorName, PolicyStartDate, PolicyEndDate, PolicyDetails, ThirdPartyAdmissionID, ADHARNo, PANNo, DoctorID1, DoctorID2, AdmittingDocName1, MLCNotes, Status, Remark, IsDischarge, DischargeDate, DischargeReason, ClaimFormNo, CoPayment, StaffNoOrEmployeeID, MembershipID, UserName, EStatus, CashlessTAT, ClientID, LocationID, Activate)
                VALUES 
                (%(UHID)s, %(ADMNo)s, %(ADMDateTime)s, %(PatientName)s, %(Gender)s, %(GSM1)s, %(DOB)s, 
                %(AgeYear)s, %(AgeMonth)s, %(AgeDay)s, %(Address)s, %(City)s, %(AdmittingDocName)s, %(Department)s, 
                %(WardName)s, %(RoomName)s, %(BedName)s, %(SponsorType)s, %(SponsorName)s, %(PolicyStartDate)s, 
                %(PolicyEndDate)s, %(PolicyDetails)s, %(ThirdPartyAdmissionID)s, %(ADHARNo)s, %(PANNo)s,
                %(DoctorID1)s, %(DoctorID2)s, %(AdmittingDocName1)s, %(MLCNotes)s, %(Status)s, %(Remark)s, 
                %(IsDischarge)s, %(DischargeDate)s, %(DischargeReason)s, %(ClaimFormNo)s, %(CoPayment)s, 
                %(StaffNoOrEmployeeID)s, %(MembershipID)s, 
                %(UserName)s, %(EStatus)s, %(CashlessTAT)s, %(ClientID)s, %(LocationID)s, %(Activate)s
                )"""

            # Define the values to be inserted
            data = {
                "UHID": 0,   
                "ADMNo": row[1],
                "ADMDateTime": row[6],   
                "PatientName": row[12],   
                "Gender": row[22],   
                "GSM1": row[15],   
                "DOB": row[18],   
                "AgeYear":  row[19],   
                "AgeMonth": row[20],   
                "AgeDay": row[21],   
                "Address": row[13],   
                "City": row[14],   
                "AdmittingDocName": row[7],
                "Department": row[4],
                "WardName": row[27],
                "RoomName": "",   
                "BedName": row[5],
                "SponsorType": row[29],   
                "SponsorName": row[28],   
                "PolicyStartDate": row[25],   
                "PolicyEndDate": row[26],   
                "PolicyDetails": row[24],   
                "ThirdPartyAdmissionID": row[0],
                "ADHARNo": row[16],   
                "PANNo": row[17],    
                "DoctorID1": 0,   
                "DoctorID2": 0,   
                "AdmittingDocName1": "",
                "MLCNotes": row[8],
                "Status": row[9],
                "Remark": "",   
                "IsDischarge": row[10],
                "DischargeDate": row[6],   
                "DischargeReason": "",   
                "ClaimFormNo": row[23],   
                "CoPayment": 0,   
                "StaffNoOrEmployeeID": row[31],   
                "MembershipID": row[32],   
                "UserName": "",   
                "EStatus": "",   
                "CashlessTAT": 0,   
                "ClientID": row[2],
                "LocationID": row[3],
                "Activate": row[11]
            }
            
            WhatsBusinessAPIsplitvalues = WhatsBusinessAPIStringCall.split('#')
            
            # Now split_values is a list containing the individual values
            # print(type (WhatsBusinessAPIsplitvalues))
            
            
            APIKEY=WhatsBusinessAPIsplitvalues[0]
            WhatsappBusinessAPI =WhatsBusinessAPIsplitvalues[1]
            UserName =WhatsBusinessAPIsplitvalues[2]
            APIUrl =WhatsBusinessAPIsplitvalues[3]
            Near_Token_CampaignName =WhatsBusinessAPIsplitvalues[4]
            OPD_Started_CampaignName =WhatsBusinessAPIsplitvalues[5]
            OPD_Cancelled_CampaignName =WhatsBusinessAPIsplitvalues[6]
            Cashless_Admission_Salus_CampaignName =WhatsBusinessAPIsplitvalues[7]
            Cashless_Admission_Salus_ContactNo =WhatsBusinessAPIsplitvalues[8]
            Cashless_Admission_Salus_ContactNos = Cashless_Admission_Salus_ContactNo.split(',')
            CampaignName =""
            
            Template_Cashless_Admission_Param=[""]*6
            async def main():
                # Your code to set up variables and environment
                if WhatsappBusinessAPI.strip().upper() == "TRUE":
                    if APIKEY.strip() and UserName.strip() and APIUrl.strip():
                        Template_Cashless_Admission_Param[0] = str(row[12]).upper()
                        Template_Cashless_Admission_Param[1] = str(row[28]).upper()
                        Template_Cashless_Admission_Param[2] = str(row[1]).upper()
                        Template_Cashless_Admission_Param[3] = str(row[6]).upper()
                        Template_Cashless_Admission_Param[4] = str(row[6]).upper()
                        Template_Cashless_Admission_Param[5] = str(row[7]).upper()
                        CampaignName = Cashless_Admission_Salus_CampaignName
                        print(Template_Cashless_Admission_Param)

                        # Await the coroutine here
                        await send_whatsapp_messages_async(Cashless_Admission_Salus_ContactNos, APIKEY, CampaignName, Template_Cashless_Admission_Param, UserName, APIUrl)

                result = "OK"
                print(result)
            asyncio.run(main())
            # # Execute the query to insert data into the trnadmission table
            # cursor1.execute(insert_query, data)
            # dest_db_connection.commit()

            # print("Value does not exist in test.trnadmission table's ThirdPartyAdmissionID column")

    # Close the cursor and connection
    cursor.close()
    conn.close()

    cursor1.close()
    dest_db_connection.close()

async def send_whatsapp_messages_async(Cashless_Admission_Salus_ContactNos, APIKey, CampaignName, Param, UserName, APIUrl):
    try:
        for Cashless_Admission_Salus_ContactNo in Cashless_Admission_Salus_ContactNos:
            await send_message_to_whatsapp_async(Cashless_Admission_Salus_ContactNo, APIKey, CampaignName, Param, UserName, APIUrl)
            print(send_message_to_whatsapp_async)
    except Exception as ex:
        return ex



async def send_message_to_whatsapp_async(Cashless_Admission_Salus_ContactNo, APIKey, CampaignName, Param, UserName, APIUrl):
    try:
        # Your code to send WhatsApp message using WhatsApp API
        response = await call_whatsapp_api_campaign(APIKey, CampaignName, Cashless_Admission_Salus_ContactNo, Param, UserName, APIUrl)
        print(response)
        await asyncio.sleep(0.1)  # Simulated delay
    except Exception as ex:
        # Assuming objCommonRepository.LogError is a method to log errors
        return ex


async def call_whatsapp_api_campaign(APIKey, CampaignName, GSM, Param, UserName, APIUrl):
    result = ''
    apiUrl = APIUrl
    GSM = '91' + GSM
    jsonData = {
        "apiKey": APIKey,
        "campaignName": CampaignName,
        "destination": GSM,
        "userName": UserName,
        "templateParams": Param,
        "source": "new-landing-page form",
        "media": {},
        "buttons": [],
        "carouselCards": [],
        "location": {}
    }
    print(jsonData)
    jsonDataString = json.dumps(jsonData)

    # URL of the WhatsApp API endpoint

    # Send the JSON data along with the request to the API URL
    response = requests.post(APIUrl, data=jsonDataString)

    # Check the response
    if response.status_code == 200:
        print("Message sent successfully!")
    else:
        print("Failed to send message. Status code:", response.status_code)


    # try:
    #     response = requests.post(apiUrl, json=jsonData)
    #     response.raise_for_status()  # Raise an error if response status is not OK (200)
    #     print(response.raise_for_status)
    #     responseObj = response.json()
    #     result = responseObj.get('success', 'false')
    #     print(result)
    # except Exception as ex:
    #     result = 'false'
    # return result


# Schedule to run every hour
schedule.every().hour.at(":00").do(save_data)

# Schedule to run once daily at midnight
schedule.every().day.at("10:20").do(save_data)
# Infinite loop to keep the script running
while True:
    schedule.run_pending()
    time.sleep(1)
