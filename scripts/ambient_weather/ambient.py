{
 "cells": [
  {
   "cell_type": "markdown",
   "id": "81a38f02",
   "metadata": {},
   "source": [
    "# Read and Process Ambient Weather Station Data\n",
    "\n",
    "This notebook will call the Ambient Weather API and extract data from weather stations using a shared API key. This key for your stations can be obtained from ambientweather.net. "
   ]
  },
  {
   "cell_type": "markdown",
   "id": "ac252fcd",
   "metadata": {},
   "source": [
    "## Import"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 1,
   "id": "0f07944b",
   "metadata": {},
   "outputs": [],
   "source": [
    "from ambient_api.ambientapi import AmbientAPI\n",
    "import numpy as np\n",
    "from datetime import datetime\n",
    "import time\n",
    "import pandas as pd\n",
    "import xarray as xr\n",
    "import os\n",
    "from pathlib import Path"
   ]
  },
  {
   "cell_type": "markdown",
   "id": "7c1be306",
   "metadata": {},
   "source": [
    "## Acess the API\n",
    "\n",
    "You will need to define how to reach the API first in your enviroment. enter it in your command line as such, replacing yourapidkeyhere and yourapplicationkeyhere with your API key and application key. \n",
    "\n",
    "export AMBIENT_ENDPOINT=https://api.ambientweather.net/v1\n",
    "\n",
    "export AMBIENT_API_KEY=\"yourapikeyhere\"\n",
    "\n",
    "export AMBIENT_APPLICATION_KEY=\"yourapplicationkeyhere\""
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 2,
   "id": "b61c5897",
   "metadata": {},
   "outputs": [],
   "source": [
    "# Access the Ambient weather API and get the devices available\n",
    "api = AmbientAPI()\n",
    "devices = api.get_devices()"
   ]
  },
  {
   "cell_type": "markdown",
   "id": "674b2df1",
   "metadata": {},
   "source": [
    "# Setting up the function and metadata"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 3,
   "id": "b69dd841",
   "metadata": {},
   "outputs": [],
   "source": [
    "attrs_dict = {'tempf':{'standard_name': 'Temperature',\n",
    "                       'units': 'degF'},\n",
    "              'tempinf':{'standard_name': 'Temperature',\n",
    "                         'units': 'degF'},\n",
    "              'feelsLike':{'standard_name': 'Feels Like Temperature',\n",
    "                        'units': 'degF'},\n",
    "              'dewPoint': {'standard_name': 'Dewpoint Temperature',\n",
    "                           'units': 'degF'},\n",
    "              'dewPointin': {'standard_name': 'Dewpoint Temperature',\n",
    "                  'units': 'degF'},\n",
    "              'windspeedmph':{'standard_name': 'Wind Speed',\n",
    "                            'units':'mph'},\n",
    "              'winddir':{'standard_name': ' Wind Direction',\n",
    "                        'units':'Degrees 0-360'},\n",
    "              'windgustmph':{'standard_name':'Wind Gust last 10 min',\n",
    "                        'units':'mph'},\n",
    "              'windgustdir':{'standard_name':'Wind direction of gust',\n",
    "                        'units':'Degrees 0-360'},\n",
    "              'hourlyrainin':{'standard_name':'Hourly Rain Rate',\n",
    "                        'units':'in/hr'},\n",
    "              'dailyrainin':{'standard_name':'Daily Rain',\n",
    "                        'units':'inches'},\n",
    "              'eventrainin':{'standard_name':'Event Rain',\n",
    "                        'units':'inches'},\n",
    "              'baromrelin':{'standard_name':'Relative Pressure',\n",
    "                        'units':'inHg'},\n",
    "              'baromabsin':{'standard_name':'Absolute Pressure',\n",
    "                        'units':'inHg'},\n",
    "              'solarradiation':{'standard_name':'Solar Radiation',\n",
    "                        'units':'W/m^2'},\n",
    "              'pm25':{'standard_name':'PM 2.5',\n",
    "                      'units':'ug/m^3'},\n",
    "              'pm25_24h':{'standard_name':'PM2.5 Air Quality 24 hour average',\n",
    "                      'units':'ug/m^3'},\n",
    "              'battout':{'standard_name':'Outdoor Battery',\n",
    "                        'units':'1=ok,0=low'},\n",
    "              'batt_25':{'standard_name':'PM 2.5 Battery Power',\n",
    "                        'units':'1=ok,0=low'}}\n",
    "\n",
    "variable_mapping = {'date':'time',\n",
    "                    'tempf':'outdoor_temperature',\n",
    "                    'tempinf':'indoor_temperature',\n",
    "                    'dewPoint':'outdoor_dewpoint',\n",
    "                    'dewPointin':'indoor_dewpoint',\n",
    "                    'feelsLike':'feelslike_temperature',\n",
    "                    'winddir':'wind_direction',\n",
    "                    'windspeedmph':'wind_speed',\n",
    "                    'windgustmph':'wind_gust',\n",
    "                    'windgustdir':'wind_gust_direction',\n",
    "                    'hourlyrainin':'hourly_rain',\n",
    "                    'dailyrainin':'daily_rain',\n",
    "                    'eventrainin':'event_rain',\n",
    "                    'baromrelin':'relative_pressure',\n",
    "                    'baromabsin':'absolute_pressure',\n",
    "                    'solarradiation':'solar_radiation',\n",
    "                    'pm25':'pm25_outdoor',\n",
    "                    'pm25_24h':'pm25_24hr',\n",
    "                    'battout':'station_battery',\n",
    "                    'batt_25':'pm25_battery'\n",
    "                    }"
   ]
  },
  {
   "cell_type": "markdown",
   "id": "86f79e8f",
   "metadata": {},
   "source": [
    "Now we create the function"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 4,
   "id": "83de155f",
   "metadata": {},
   "outputs": [],
   "source": [
    "def process_station(device, attrs=attrs_dict, variable_mapping=variable_mapping):\n",
    "\n",
    "    current_date = datetime.utcnow()\n",
    "    # Read in the station data\n",
    "    data = device.get_data(end_date = current_date)\n",
    "\n",
    "    meta = device.info\n",
    "\n",
    "    # Read into a pandas dataframe\n",
    "    df = pd.DataFrame(data)\n",
    "    # Format the times properly\n",
    "    df['date'] = pd.to_datetime(df.date).astype('datetime64[ns]')\n",
    "\n",
    "    # Sort the values and set the index to be the date\n",
    "    df = df.sort_values('date')\n",
    "    df = df.set_index('date')\n",
    "\n",
    "    ds = df.to_xarray()\n",
    "    #print(df,ds)\n",
    "    # Add associated metadata\n",
    "    for variable in attrs.keys():\n",
    "        if variable in list(ds.variables):\n",
    "            ds[variable].attrs = attrs[variable]\n",
    "\n",
    "       # print (ds[variable])\n",
    "    # Rename the variables\n",
    "    #print (sorted(list(ds.variables)),sorted(list(variable_mapping.keys())))\n",
    "    theirvariables = sorted(list(ds.variables))\n",
    "    ourvariables = sorted(list(variable_mapping.keys()))\n",
    "    sharedvariables = dict()\n",
    "    for variable in theirvariables:\n",
    "        if variable in ourvariables:\n",
    "            sharedvariables[variable]=variable_mapping[variable]\n",
    "\n",
    "\n",
    "    ds = ds.rename(sharedvariables)\n",
    "\n",
    "    # Reshape the data\n",
    "    ds = ds.expand_dims('station')\n",
    "    ds['station'] = [meta['name']]\n",
    "    ds['latitude'] = meta['coords']['coords']['lat']\n",
    "    ds['longitude'] = meta['coords']['coords']['lon']\n",
    "\n",
    "    ds = ds.sel(time=f\"{current_date.year}-{current_date.month}-{current_date.day}\")\n",
    "\n",
    "    return ds"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 5,
   "id": "5eb7954a",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "CMS-AMB-001@30:83:98:A5:AB:EC\n"
     ]
    },
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "/home/mtuftedal/anaconda3/lib/python3.7/site-packages/ipykernel_launcher.py:12: FutureWarning: Using .astype to convert from timezone-aware dtype to timezone-naive dtype is deprecated and will raise in a future version.  Use obj.tz_localize(None) or obj.tz_convert('UTC').tz_localize(None) instead\n",
      "  if sys.path[0] == '':\n"
     ]
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "CMS-AMB-002@30:83:98:A7:57:C2\n",
      "CMS-AMB-004@C4:5B:BE:6E:11:CF\n",
      "[<xarray.Dataset>\n",
      "Dimensions:                (time: 217, station: 1)\n",
      "Coordinates:\n",
      "  * time                   (time) datetime64[ns] 2023-02-06T02:15:00 ... 2023...\n",
      "  * station                (station) <U11 'CMS-AMB-001'\n",
      "Data variables: (12/30)\n",
      "    dateutc                (station, time) int64 1675649700000 ... 1675714500000\n",
      "    outdoor_temperature    (station, time) float64 30.7 30.7 30.6 ... 43.0 43.0\n",
      "    humidity               (station, time) int64 83 83 83 83 83 ... 63 61 61 61\n",
      "    wind_speed             (station, time) float64 2.46 2.24 2.91 ... 10.51 6.04\n",
      "    wind_gust              (station, time) float64 3.36 3.36 3.36 ... 12.53 6.93\n",
      "    maxdailygust           (station, time) float64 25.05 25.05 ... 17.22 17.22\n",
      "    ...                     ...\n",
      "    outdoor_dewpoint       (station, time) float64 26.16 26.16 ... 30.48 30.48\n",
      "    feelsLikein            (station, time) float64 66.3 67.5 67.2 ... 80.3 81.0\n",
      "    indoor_dewpoint        (station, time) float64 27.2 28.1 27.9 ... 33.5 34.3\n",
      "    lastRain               (station, time) object '2023-02-03T04:11:00.000Z' ...\n",
      "    latitude               float64 41.7\n",
      "    longitude              float64 -88.0, <xarray.Dataset>\n",
      "Dimensions:                (time: 217, station: 1)\n",
      "Coordinates:\n",
      "  * time                   (time) datetime64[ns] 2023-02-06T02:15:00 ... 2023...\n",
      "  * station                (station) <U11 'CMS-AMB-002'\n",
      "Data variables: (12/30)\n",
      "    dateutc                (station, time) int64 1675649700000 ... 1675714500000\n",
      "    outdoor_temperature    (station, time) float64 30.6 30.4 30.2 ... 43.2 42.8\n",
      "    humidity               (station, time) int64 83 83 84 83 84 ... 62 62 61 61\n",
      "    wind_speed             (station, time) float64 2.46 2.91 2.46 ... 5.37 6.93\n",
      "    wind_gust              (station, time) float64 3.36 3.36 3.36 ... 6.93 9.17\n",
      "    maxdailygust           (station, time) float64 23.93 23.93 ... 14.76 14.76\n",
      "    ...                     ...\n",
      "    outdoor_dewpoint       (station, time) float64 26.06 25.87 ... 30.67 30.29\n",
      "    feelsLikein            (station, time) float64 68.0 65.8 66.3 ... 85.2 86.8\n",
      "    indoor_dewpoint        (station, time) float64 26.6 27.9 27.2 ... 37.0 38.5\n",
      "    lastRain               (station, time) object '2023-02-02T19:28:00.000Z' ...\n",
      "    latitude               float64 41.7\n",
      "    longitude              float64 -88.0, <xarray.Dataset>\n",
      "Dimensions:                (time: 216, station: 1)\n",
      "Coordinates:\n",
      "  * time                   (time) datetime64[ns] 2023-02-06T02:20:00 ... 2023...\n",
      "  * station                (station) <U11 'CMS-AMB-004'\n",
      "Data variables: (12/30)\n",
      "    dateutc                (station, time) int64 1675650000000 ... 1675714500000\n",
      "    outdoor_temperature    (station, time) float64 30.0 29.8 30.2 ... 42.8 42.6\n",
      "    humidity               (station, time) int64 84 84 83 84 84 ... 63 61 62 61\n",
      "    wind_speed             (station, time) float64 1.34 1.79 2.24 ... 7.83 4.03\n",
      "    wind_gust              (station, time) float64 2.24 2.24 2.24 ... 9.17 5.82\n",
      "    maxdailygust           (station, time) float64 20.58 20.58 ... 14.76 17.22\n",
      "    ...                     ...\n",
      "    outdoor_dewpoint       (station, time) float64 25.76 25.57 ... 30.69 30.11\n",
      "    feelsLikein            (station, time) float64 67.8 66.0 ... 102.2 100.7\n",
      "    indoor_dewpoint        (station, time) float64 26.4 27.0 26.8 ... 41.6 40.6\n",
      "    lastRain               (station, time) object '2023-02-04T18:38:00.000Z' ...\n",
      "    latitude               float64 41.7\n",
      "    longitude              float64 -88.0]\n"
     ]
    }
   ],
   "source": [
    "# Loop through for each device and retrieve the data, waiting for the API to clean up first\n",
    "dsets = []\n",
    "for device in devices:\n",
    "    try:\n",
    "        print(device)\n",
    "        dsets.append(process_station(device))\n",
    "\n",
    "    except:\n",
    "        pass\n",
    "    time.sleep(10)\n",
    "print (dsets)\n",
    "ds = xr.concat(dsets, dim='station')"
   ]
  },
  {
   "cell_type": "markdown",
   "id": "1891d382",
   "metadata": {},
   "source": [
    "## Writing out the data file"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 6,
   "id": "dc918734",
   "metadata": {},
   "outputs": [],
   "source": [
    "end_time = ds.isel(time=-1)\n",
    "time_label = pd.to_datetime(end_time.time.values).strftime('%Y/%m/%d/ambient.a1.%Y%m%d.nc')\n",
    "full_file = f'/media/sf_virtual_machine_shared/Ambient/data/{time_label}'\n",
    "full_path = Path(full_file)\n",
    "if not os.path.exists(full_path.parent):\n",
    "    os.makedirs(full_path.parent)\n",
    "ds.to_netcdf(full_file)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "8c1d9bca",
   "metadata": {},
   "outputs": [],
   "source": []
  }
 ],
 "metadata": {
  "kernelspec": {
   "display_name": "Python 3",
   "language": "python",
   "name": "python3"
  },
  "language_info": {
   "codemirror_mode": {
    "name": "ipython",
    "version": 3
   },
   "file_extension": ".py",
   "mimetype": "text/x-python",
   "name": "python",
   "nbconvert_exporter": "python",
   "pygments_lexer": "ipython3",
   "version": "3.7.6"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 5
}
