# -*- encoding: utf-8 -*-

from json2html import *
import urllib2
import json
from bs4 import BeautifulSoup
import psycopg2
import boto3
import os

def lambda_handler(event, context):
    title = "Flight Radar SI"
    url = "https://data.flightradar24.com/zones/fcgi/europe_all.js"
    hdr = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.64 Safari/537.11',
           'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
           'Accept-Charset': 'ISO-8859-1,utf-8;q=0.7,*;q=0.3',
           'Accept-Encoding': 'none',
           'Accept-Language': 'en-US,en;q=0.8',
           'Connection': 'keep-alive'}
    geonames = "http://api.geonames.org/countrySubdivisionJSON?lat=%s&lng=%s&username=FlightCampus"
    was = event['type']
    if was == 'start':
        req = urllib2.Request(url, headers=hdr)
        response = urllib2.urlopen(req)
        data_url_json = json.load(response)
        for key, val in data_url_json.iteritems():
            if key == 'full_count' or key == 'version':
                continue
            data_url_json[key] = {'MODE-S CODE':val[0],'LATITUDE':val[1],'LONGITUDE':val[2],'TRACK':val[3],'CALIBRATED ALTITUDE':val[4],'GROUND SPEED':val[5],'SQUAWK':val[6],'RADAR':val[7],'TYPE':val[8],'REGISTRATION':val[9],'SERIAL':val[10],'FROM':val[11],'TO':val[12],'FLIGHT CODE':val[13],'FLIGHT CODE2':val[16]}
        html = json2html.convert(json = data_url_json, table_attributes="class=\"table table-bordered table-hover\"", clubbing=True)
        soup = BeautifulSoup(html, 'html.parser')
        html = ""
        for table in soup.find_all('table')[1:]:
            table = table.wrap(soup.new_tag('div',**{'class':'col-md-3 col-xs-12 col-sm-6'}))
            html += str(table.prettify())
        html ='<div class="container-fluid"><nav class="navbar navbar-collapse navbar-default navbar-fixed-top"><div class="container-fluid"><div class="row"><p class="navbar-text">Aktionen:</p><div class="btn-group" role="group"><a href="https://ygdm6wub2a.execute-api.us-east-1.amazonaws.com/flightradar/start"><button type="button" class="btn btn-default">Hauptseite</button></a><a href="https://ygdm6wub2a.execute-api.us-east-1.amazonaws.com/flightradar/datenbank/flugzeug-de"><button type="button" class="btn btn-default">Flugzeuge über Deutschland</button></a><a href="https://ygdm6wub2a.execute-api.us-east-1.amazonaws.com/flightradar/datenbank/db-anzeigen"><button type="button" class="btn btn-default">Datenbank anzeigen</button></a><a href="https://ygdm6wub2a.execute-api.us-east-1.amazonaws.com/flightradar/notfaelle"><button type="button" class="btn btn-default">Notfälle prüfen</button></a></div></div></div></nav><div class="row">'+html+'</div></div>'
    if was == 'flugzeug_de':
        req = urllib2.Request(url, headers=hdr)
        response = urllib2.urlopen(req)
        data_url_json = json.load(response)
        conn_param = "dbname='postgres_cloud' user="+os.environ['DB_USER']+" host="+os.environ['DB_HOST']+" password="+os.environ['DB_PASSWORD']
        conn = psycopg2.connect(conn_param)
        cur = conn.cursor()
        count = 0
        g_count = 0
        for key, val in data_url_json.iteritems():
            g_count += 1
            if g_count > 1000:
                break
            try:
                geo_request = geonames % (str(val[1]), str(val[2]))
            except:
                continue
            req2 = urllib2.Request(geo_request)
            response_geo = urllib2.urlopen(req2)
            geo_json = json.load(response_geo)
            try:
                if geo_json['countryName'] == 'Germany':
                    count +=1
                    cur.execute("""insert into flightradar.germany (mcode,latitude,longitude,track,caltitude,gspeed,squawk,radar,"type",registration,serial,"from","to",flightcode,flightcode2,zeit)
                    values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,current_timestamp)""",(val[0],float(val[1]),float(val[2]),str(val[3]),int(val[4]),int(val[5]),int(val[6]),str(val[7]),str(val[8]),str(val[9]),str(val[10]),str(val[11]),str(val[12]),str(val[13]),str(val[16])))
            except:
                continue
        conn.commit()
        cur.close()
        conn.close()
        html = '<p>'+str(count)+' Flugzeugen befinden sich in Deutschland.</p>'
        html ='<div class="container-fluid"><nav class="navbar navbar-collapse navbar-default navbar-fixed-top"><div class="container-fluid"><div class="row"><p class="navbar-text">Aktionen:</p><div class="btn-group" role="group"><a href="https://ygdm6wub2a.execute-api.us-east-1.amazonaws.com/flightradar/start"><button type="button" class="btn btn-default">Hauptseite</button></a><a href="https://ygdm6wub2a.execute-api.us-east-1.amazonaws.com/flightradar/datenbank/flugzeug-de"><button type="button" class="btn btn-default">Flugzeuge über Deutschland</button></a><a href="https://ygdm6wub2a.execute-api.us-east-1.amazonaws.com/flightradar/datenbank/db-anzeigen"><button type="button" class="btn btn-default">Datenbank anzeigen</button></a><a href="https://ygdm6wub2a.execute-api.us-east-1.amazonaws.com/flightradar/notfaelle"><button type="button" class="btn btn-default">Notfälle prüfen</button></a></div></div></div></nav><div class="row">'+html+'</div></div>'
    if was == 'db_show':
        conn_param = "dbname='postgres_cloud' user="+os.environ['DB_USER']+" host="+os.environ['DB_HOST']+" password="+os.environ['DB_PASSWORD']
        conn = psycopg2.connect(conn_param)
        cur = conn.cursor()
        cur.execute("""select * from flightradar.germany limit 500""")
        records = cur.fetchall()
        cur.close()
        conn.close()
        html='<table class="table table-bordered table-hover">'
        html+='<tr><th>ID</th><th>MODE-S CODE</th><th>LATITUDE</th><th>LONGITUDE</th><th>TRACK</th><th>CALIBRATED ALTITUDE</th><th>GROUND SPEED</th><th>SQUAWK</th><th>RADAR</th><th>TYPE</th><th>REGISTRATION</th><th>SERIAl</th><th>FROM</th><th>TO</th><th>FLIGHT CODE</th><th>FLIGHT CODE 2</th><th>ZEIT</th></tr>'
        for each in records:
            html+='<tr><td>'+str(each[0])+'</td><td>'+each[1]+'</td><td>'+str(each[2])+'</td><td>'+str(each[3])+'</td><td>'+each[4]+'</td><td>'+str(each[5])+'</td><td>'+str(each[6])+'</td><td>'+str(each[7])+'</td><td>'+each[8]+'</td><td>'+each[9]+'</td><td>'+each[10]+'</td><td>'+each[11]+'</td><td>'+each[12]+'</td><td>'+each[13]+'</td><td>'+each[14]+'</td><td>'+each[15]+'</td><td>'+str(each[16])+'</td></tr>'
        html+='</table>'
        html ='<div class="container-fluid"><nav class="navbar navbar-collapse navbar-default navbar-fixed-top"><div class="container-fluid"><div class="row"><p class="navbar-text">Aktionen:</p><div class="btn-group" role="group"><a href="https://ygdm6wub2a.execute-api.us-east-1.amazonaws.com/flightradar/start"><button type="button" class="btn btn-default">Hauptseite</button></a><a href="https://ygdm6wub2a.execute-api.us-east-1.amazonaws.com/flightradar/datenbank/flugzeug-de"><button type="button" class="btn btn-default">Flugzeuge über Deutschland</button></a><a href="https://ygdm6wub2a.execute-api.us-east-1.amazonaws.com/flightradar/datenbank/db-anzeigen"><button type="button" class="btn btn-default">Datenbank anzeigen</button></a><a href="https://ygdm6wub2a.execute-api.us-east-1.amazonaws.com/flightradar/notfaelle"><button type="button" class="btn btn-default">Notfälle prüfen</button></a></div></div></div></nav><div class="row">'+html+'</div></div>'
    if was == 'notfaelle':
        req = urllib2.Request(url, headers=hdr)
        response = urllib2.urlopen(req)
        data_url_json = json.load(response)
        toEmail = os.environ['toemail']
        fromEmail = os.environ['fromemail']
        replyTo = os.environ['replyto']
        subject = "Flight Radar Notfall"
        client = boto3.client('ses')
        count = 0
        for key, val in data_url_json.iteritems():
            try:
                if val[6] == '7500' or val[6] == '7600' or val[6] == '7700':
                    count +=1
                    message = 'MODE-S CODE:'+val[0]+'\nLATITUDE:'+val[1]+'\nLONGITUDE:'+val[2]+'\nTRACK:'+val[3]+'\nCALIBRATED ALTITUDE:'+val[4]+'\nGROUND SPEED:'+val[5]+'\nSQUAWK:'+val[6]+'\nRADAR:'+val[7]+'\nTYPE:'+val[8]+'\nREGISTRATION:'+val[9]+'\nSERIAL:'+val[10]+'\nFROM:'+val[11]+'\nTO:'+val[12]+'\nFLIGHT CODE:'+val[13]+'\nFLIGHT CODE2:'+val[16]
                    response = client.send_email(
                        Source=fromEmail,
                        Destination={
                            'ToAddresses': [
                                toEmail,
                            ],
                        },
                        Message={
                            'Subject': {
                                'Data': subject,
                                'Charset': 'utf8'
                            },
                            'Body': {
                                'Text': {
                                    'Data': message,
                                    'Charset': 'utf8'
                                }
                            }
                        }
                    )
            except:
                continue
        html = '<p>'+str(count)+' Notfälle wurden gefunden. '+str(count)+' E-Mails wurden geschickt.'
        html ='<div class="container-fluid"><nav class="navbar navbar-collapse navbar-default navbar-fixed-top"><div class="container-fluid"><div class="row"><p class="navbar-text">Aktionen:</p><div class="btn-group" role="group"><a href="https://ygdm6wub2a.execute-api.us-east-1.amazonaws.com/flightradar/start"><button type="button" class="btn btn-default">Hauptseite</button></a><a href="https://ygdm6wub2a.execute-api.us-east-1.amazonaws.com/flightradar/datenbank/flugzeug-de"><button type="button" class="btn btn-default">Flugzeuge über Deutschland</button></a><a href="https://ygdm6wub2a.execute-api.us-east-1.amazonaws.com/flightradar/datenbank/db-anzeigen"><button type="button" class="btn btn-default">Datenbank anzeigen</button></a><a href="https://ygdm6wub2a.execute-api.us-east-1.amazonaws.com/flightradar/notfaelle"><button type="button" class="btn btn-default">Notfälle prüfen</button></a></div></div></div></nav><div class="row">'+html+'</div></div>'
    return {"title": title, "html": html}