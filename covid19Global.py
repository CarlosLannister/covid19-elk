import requests
from datetime import datetime, date, timedelta
from countryinfo import CountryInfo
from elasticsearch import Elasticsearch

def getInfectionRate(confirmed, population):
    infectionRate = 100 * (confirmed / population)   
    return float(infectionRate)

def save_elasticsearch_es(index, result_data):
    es = Elasticsearch(hosts="") #Your auth data

    es.indices.create(
        index=index,
        ignore=400  # ignore 400 already exists code
    )

    id_case = str(result_data['timestamp'].strftime("%d-%m-%Y")) + \
        '-'+result_data['name']
    es.update(index=index, id=id_case, body={'doc':result_data,'doc_as_upsert':True})

def main():
    try:
        start_date = date(2020, 3, 1)
        end_date = date(2020, 4, 9)
        delta = timedelta(days=1)
        while start_date <= end_date:  
            day = start_date.strftime("%Y-%m-%d")  
            print ("Downloading " + day)    
            url = "https://api.covid19tracking.narrativa.com/api/" + day
            r = requests.get(url)
            data = r.json()    
            start_date += delta

            for day in data['dates']:
                for country in data['dates'][day]['countries']:
                    try:
                        country_info = CountryInfo(country)
                        country_iso_3 = country_info.iso(3)
                        population = country_info.population()
                    except Exception as e:
                        print("Error with " + country)
                        country_iso_3 = country
                        population = None
                        infection_rate=0
                        print(e)

            if population != None:
                try:
                    infection_rate=getInfectionRate(data['dates'][day]['countries'][country]['today_confirmed'], population)
                    print(infection_rate)
                except:
                    infection_rate=0

            result_data = data['dates'][day]['countries'][country]
            del result_data['regions']
            result_data['timestamp'] = result_data.pop('date')
            result_data.update(
                        timestamp=datetime.strptime(day, "%Y-%m-%d"),
                        country_iso_3=country_iso_3,
                        population=population,
                        infection_rate=infection_rate,
                        )

            print(result_data)
            save_elasticsearch_es('covid-19-live-global',result_data)
    except Exception as e:
        print(e)

if __name__ == "__main__":
    main()