import logging
import psycopg2
from sendgrid.helpers.mail import Mail
from datetime import datetime

import azure.functions as func

def main(msg: func.ServiceBusMessage):

    notification_id = int(msg.get_body().decode('utf-8'))
    logging.info('Python ServiceBus queue trigger processed message: %s',notification_id)

    # TODO: Get connection to database
    connection = psycopg2.connect(dbname="prj3techconfdb", user="prj3hakey94@prj3techconfdb", password="123456789aA@", host="prj3techconfdb.postgres.database.azure.com")
    # open connection string
    cursor = connection.cursor()
    try:
        # TODO: Get notification message and subject from database using the notification_id
        query = cursor.execute("SELECT message, subject FROM notification WHERE id = {};".format(notification_id))

        # TODO: Get attendees email and name
        currentDate = datetime.utcnow()
        cursor.execute("SELECT first_name, last_name, email FROM attendee;")
        
        # TODO: Loop through each attendee and send an email with a personalized subject
        count = 0
        for attendee in cursor.fetchall():
            count += 1
            first_name = attendee[0]
            last_name = attendee[1]
            email = attendee[2]
        
            from_email = "admin@hakey94.com"

            Mail('{}, {}, {}'.format({from_email}, {email}, {query}))

        # TODO: Update the notification table by setting the completed date and updating the status with the total number of attendees notified
        status = f"Notified {str(count)} attendees"
        cursor.execute("UPDATE notification SET status = '{}', completed_date = '{}' WHERE id = {};".format(status, currentDate, notification_id))        

        connection.commit()

    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(error)
    finally:
        # TODO: Close connection
        cursor.close()
        connection.close()
