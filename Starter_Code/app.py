import numpy as np

import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func

import numpy as np
import pandas as pd
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
            f"/api/v1.0/stations<br/>"
            f"/api/v1.0/tobs<br/>"
            f"/api/v1.0/<start><br/>"
            f"/api/v1.0/<start>/<end><br/>"
        )

@app.route("/api/v1.0/precipitation")
def precipitation():
        
    session = Session(engine)
    
    #Grab the measurment data 
    results = session.query(measurement.date,measurement.prcp).all()
    
    session.close()

    prec_list = []
    for date,prcp in results:
        prec_dict = {}
        prec_dict['Date'] = date
        prec_dict['Prcp'] = prcp
        prec_list.append(prec_dict)
    
    return jsonify(prec_list)

@app.route("/api/v1.0/stations")
def station_list():
        
    session = Session(engine)
    results = session.query(Station.station,Station.name)
    session.close()

    station_list = []
    for station,name in results:
        station_dict = {}
        station_dict['Station'] = station
        station_dict['Name'] = name
        station_list.append(station_dict)
    
    return jsonify(station_list)

@app.route("/api/v1.0/tobs")
def tobs():
    
    session = Session(engine)
    #Find the most active station
    sel = [measurement.station,
    func.count(measurement.station)]

    active_station = session.query(measurement.station,func.count(measurement.station)).\
    group_by(measurement.station).\
    order_by(func.count(measurement.station).desc())

    most_active = active_station.first()

    #Filter the data for station matching the most active station 

    sel = [measurement.station,
        func.min(measurement.tobs),
        func.max(measurement.tobs),
        func.avg(measurement.tobs)]

    most_active_temp = session.query(*sel).\
        filter(measurement.station==most_active.station)
    
    #Find the most recent year entry for the most active station
    ms_recent_date = session.query(measurement).\
        filter(measurement.station == most_active.station).\
        order_by(measurement.date.desc()).first().date


    ms_recent_date = dt.datetime.strptime(ms_recent_date,'%Y-%m-%d')

    ms_year_min = ms_recent_date - dt.timedelta(days=365)
    ms_year_min = ms_year_min.strftime('%Y-%m-%d')

    # Using the most active station id
    # Query the last 12 months of temperature observation data for this station and plot the results as a histogram
    sel = [measurement.tobs,
        func.count(measurement.tobs).label('Frequency')]


    top_active_station_data = session.query(measurement.date,measurement.tobs).\
        filter(measurement.station == most_active.station).\
        filter(measurement.date >= ms_year_min).\
        filter(measurement.date <= ms_recent_date).all()
        

    most_active_list = []
    for date,tobs in top_active_station_data:
        most_active_dict = {}
        most_active_dict['Date'] = date
        most_active_dict['Tobs'] = tobs
        most_active_list.append(most_active_dict)
    
    return jsonify(most_active_list)


@app.route("/api/v1.0/<start>")
def specific_start(start):
        
    session = Session(engine)
    
    sel = [func.min(measurement.tobs),
    func.max(measurement.tobs),
    func.avg(measurement.tobs)]

    #Converts the start string into a date format
    date_conversion = dt.datetime.strptime(start,'%Y-%m-%d')

    results = session.query(*sel).\
        filter(measurement.date >= date_conversion )

    tob_summary = []
    for min,max,avg in results:
        tobs_dict = {}
        tobs_dict['Min'] = min
        tobs_dict['Max'] = max
        tobs_dict['Avg'] = avg
        tob_summary.append(tobs_dict)
    
    return jsonify(tob_summary)


@app.route("/api/v1.0/<start>/<end>")
def date_range(start,end):
    start_date_conversion = dt.datetime.strptime(start,'%Y-%m-%d')
    end_date_conversion = dt.datetime.strptime(end,'%Y-%m-%d')

    session = Session(engine)
    
    sel = [func.min(measurement.tobs),
    func.max(measurement.tobs),
    func.avg(measurement.tobs)]

    #Converts the start string into a date format
    date_conversion = dt.datetime.strptime(start,'%Y-%m-%d')

    results = session.query(*sel).\
        filter(measurement.date >= start_date_conversion).\
        filter(measurement.date >= end_date_conversion)

    tob_summary = []
    for min,max,avg in results:
        tobs_dict = {}
        tobs_dict['Min'] = min
        tobs_dict['Max'] = max
        tobs_dict['Avg'] = avg
        tob_summary.append(tobs_dict)
    return jsonify(tob_summary)


if __name__ == "__main__":
    app.run(debug=True)