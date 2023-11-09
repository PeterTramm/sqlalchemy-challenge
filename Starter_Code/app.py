import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func

import datetime as dt

from flask import Flask, jsonify


#################################################
# Database Setup
#################################################
engine = create_engine("sqlite:///Resources/hawaii.sqlite")

# reflect an existing database into a new model
Base = automap_base()
# reflect the tables
Base.prepare(autoload_with=engine)

# Save reference to the table
measurement = Base.classes.measurement

Station = Base.classes.station

#################################################
# Flask Setup
#################################################
app = Flask(__name__)


#################################################
# Flask Routes
#################################################

@app.route("/")
def home():
        return (
            f"/api/v1.0/precipitation<br/>"
            f"Retuns precipitation data for each date<br/><br/>"
            f"/api/v1.0/stations<br/>"
            f"Returns all station code and station name<br/><br/>"
            f"/api/v1.0/tobs<br/>"
            f"Returns the most recent year data of precipitation for the most active station<br/><br/>"
            f"/api/v1.0/[yyyy-mm-dd]<br/>"
            f"Returns the Min, Max & Avg for precipitation after the date inclusive <br/><br/>"
            f"/api/v1.0/[yyyy-mm-dd]/[yyyy-mm-dd]<br/>"
            f"Returns the Min, Max & Avg for precipitation of the date range<br/><br/>"
        )

@app.route("/api/v1.0/precipitation")
def precipitation():

    #Open session for query    
    session = Session(engine)
    
    #Grab the measurment data 
    results = session.query(measurement.date,measurement.prcp).all()
    
    #Close session
    session.close()

    #Turns the data into a list to be jsonified 
    prec_list = []
    #Create a dict and insert values into their respective keys
    for date,prcp in results:
        prec_dict = {}
        prec_dict['Date'] = date
        prec_dict['Prcp'] = prcp
        #Append the dict into the list to be jsonified
        prec_list.append(prec_dict)
    
    return jsonify(prec_list)

@app.route("/api/v1.0/stations")
def station_list():

    #Open session for query    
    session = Session(engine)
    #Grab station name and station list
    results = session.query(Station.station,Station.name)
    #Close session
    session.close()

    #Turns the data into a list to be jsonified
    station_list = []
    #Create a dict and insert values into their respective keys
    for station,name in results:
        station_dict = {}
        station_dict['Station'] = station
        station_dict['Name'] = name
        #Append the dict into the list to be jsonified
        station_list.append(station_dict)
    
    return jsonify(station_list)

@app.route("/api/v1.0/tobs")
def tobs():
    
    #Open session for query
    session = Session(engine)

    ####################
    # Find the most active station
    ####################

    #Order station by their frequency descending 
    active_station = session.query(*sel).\
    group_by(measurement.station).\
    order_by(func.count(measurement.station).desc())

    #Grabs the most frequent station and store in a variable
    most_active = active_station.first()
    
    #Find the most recent year entry for the most active station
    ms_recent_date = session.query(measurement).\
        filter(measurement.station == most_active.station).\
        order_by(measurement.date.desc()).first().date

    #Converts the date string into a date format
    ms_recent_date = dt.datetime.strptime(ms_recent_date,'%Y-%m-%d')

    #Calculate the date that is a year from the most recent year entry
    ms_year_min = ms_recent_date - dt.timedelta(days=365)

    #Converts the date format into a string
    ms_year_min = ms_year_min.strftime('%Y-%m-%d')

    #Selecting information we want
    sel = [measurement.date,
           measurement.tobs]

    #Filtering on conditions
    top_active_station_data = session.query(*sel).\
        filter(measurement.station == most_active.station).\
        filter(measurement.date >= ms_year_min).\
        filter(measurement.date <= ms_recent_date).all()
        
    #Turns the data into a list to be jsonified
    most_active_list = []

     #Create a dict and insert values into their respective keys
    for date,tobs in top_active_station_data:
        most_active_dict = {}
        most_active_dict['Date'] = date
        most_active_dict['Tobs'] = tobs

        #Append the dict into the list to be jsonified
        most_active_list.append(most_active_dict)
    
    return jsonify(most_active_list)


@app.route("/api/v1.0/<start>")
def specific_start(start):
    
    #open session for query    
    session = Session(engine)
    
    #Selecting data
    sel = [func.min(measurement.tobs),
    func.max(measurement.tobs),
    func.avg(measurement.tobs)]

    #Converts the start string into a date format
    date_conversion = dt.datetime.strptime(start,'%Y-%m-%d')
    
    #Query data on condition
    results = session.query(*sel).\
        filter(measurement.date >= date_conversion )
    
    #close session
    session.close()

    #Turns the data into a list to be jsonified
    tob_summary = []
    #Create a dict and insert values into their respective keys
    for min,max,avg in results:
        tobs_dict = {}
        tobs_dict['Min'] = min
        tobs_dict['Max'] = max
        tobs_dict['Avg'] = avg
        
        #Append the dict into the list to be jsonified
        tob_summary.append(tobs_dict)
    
    return jsonify(tob_summary)


@app.route("/api/v1.0/<start>/<end>")
def date_range(start,end):
    #Converts both dates strings into date datatype
    start_date_conversion = dt.datetime.strptime(start,'%Y-%m-%d')
    end_date_conversion = dt.datetime.strptime(end,'%Y-%m-%d')

    #Open session for query
    session = Session(engine)
    
    #Selectiing data
    sel = [func.min(measurement.tobs),
    func.max(measurement.tobs),
    func.avg(measurement.tobs)]

    #Converts the start string into a date format
    date_conversion = dt.datetime.strptime(start,'%Y-%m-%d')

    #Query data on condition
    results = session.query(*sel).\
        filter(measurement.date >= start_date_conversion).\
        filter(measurement.date >= end_date_conversion)

    #Close session
    session.close()

    ##Turns the data into a list to be jsonified
    tob_summary = []
    #Create a dict and insert values into their respective keys
    for min,max,avg in results:
        tobs_dict = {}
        tobs_dict['Min'] = min
        tobs_dict['Max'] = max
        tobs_dict['Avg'] = avg
         #Append the dict intogi the list to be jsonified
        tob_summary.append(tobs_dict)

    return jsonify(tob_summary)


if __name__ == "__main__":
    app.run(debug=True)