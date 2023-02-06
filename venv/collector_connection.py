from bs4 import BeautifulSoup
import requests
import lxml
import datetime , time
import json
from calendar import timegm

import ingest_api as all_ingest_api
import production_ingest_api as production_ingest

def cc_auctions():

    page = True
    page_no = 0
    status = None
    opening_bid = None

    error_count = 0
    asset_counter = 0
    result_list = []
    crawl_status = None
    status_msg = None

    prod_error_count = 0
    prod_asset_counter = 0
    prod_crawl_status = None
    prod_result_list = []

    crawl_started = all_ingest_api.start_crawl()
    prod_crawl_started = production_ingest.start_crawl()
    print("crawl_started", crawl_started['id'])

    try:
        new_session = requests.session()
        while(page):
            while_loop_checker = True
            page_no = page_no + 1
            print(page_no)
            url = "https://thecollectorconnection.com/Lots/Gallery?page=" + str(page_no)
            auction_resp = new_session.get(url)
            print(auction_resp)

            auction_details = BeautifulSoup(auction_resp.text, 'lxml')

            auction_name = auction_details.find(class_="sidebar-widget").h5.text.strip()
            # print("auction_name" , auction_name)

            auction_data = auction_details.find_all(class_='col-lg-3 col-md-4 col-sm-6')
            # print("auction_data" , type(auction_data))
            for each_auction_data in auction_data:
                # print("inside for")
                lot_number = each_auction_data.find(class_='item').h5.text
                # print("lot_number" , lot_number )
                asset_link = each_auction_data.find(class_='item-details clearfix').p.a['href']
                asset_id = asset_link.split("=")[1]
                # print(asset_id)
                status_date = each_auction_data.find(class_='item-details clearfix')
                status_date.find_all("p")
                for ind , each_p_tag in enumerate(status_date):
                    if ind == 5:
                        status_details = " ".join(each_p_tag.text.split())
                        # print("each_p_tag" , status_details.split(" "))
                        status = status_details.split(" ")[6]
                        opening_bid = int(status_details.split(" ")[4].replace("$", ""))

                # print( "status" , status)
                # print("opening_bid" , opening_bid)

                resp_details , prod_resp_details = auction_asset_parser(asset_link, asset_id, lot_number , opening_bid , status, auction_name)
                # print(resp_status)

                if resp_details["status"] == "success":
                    print("if success")
                    asset_counter = asset_counter + 1
                    print("asset_counter" , asset_counter)
                    result = {
                        "itemid": resp_details["asset_id"],
                        "itemdetailsendpoint": resp_details["asset_link"],
                        "itemcrawlstatus": resp_details["status"],
                        "error": resp_details["error"],
                        "meta": {
                            "platformassetid": resp_details["asset_id"],
                            "pcid": resp_details["pc_id"],
                            "itemname": resp_details["name"],
                            "offerst": resp_details["offering_start"],
                            "offerend": resp_details["offering_end"]
                        }
                    }
                    result_list.append(result)
                else:
                    print("else error")
                    error_count = error_count + 1
                    print("error_count" , error_count)
                    result = {
                        "itemid": resp_details["asset_id"],
                        "itemdetailsendpoint": resp_details["asset_link"],
                        "itemcrawlstatus": resp_details["status"],
                        "error": resp_details["error"],
                        "meta": {
                            "platformassetid": resp_details["asset_id"],
                            "pcid": resp_details["pc_id"],
                            "itemname": resp_details["name"],
                            "offerst": resp_details["offering_start"],
                            "offerend": resp_details["offering_end"]
                        }
                    }
                    result_list.append(result)

                if prod_resp_details["status"] == "success":
                    print("if success")
                    prod_asset_counter = prod_asset_counter + 1
                    prod_result = {
                        "itemid": prod_resp_details["asset_id"],
                        "itemdetailsendpoint": prod_resp_details["asset_link"],
                        "itemcrawlstatus": prod_resp_details["status"],
                        "error": prod_resp_details["error"],
                        "meta": {
                            "platformassetid": prod_resp_details["asset_id"],
                            "pcid": prod_resp_details["pc_id"],
                            "itemname": prod_resp_details["name"],
                            "offerst": prod_resp_details["offering_start"],
                            "offerend": prod_resp_details["offering_end"]
                        }
                    }
                    prod_result_list.append(prod_result)

                elif prod_resp_details["status"] == "error":
                    print("if error")
                    prod_error_count = prod_error_count + 1
                    prod_result = {
                        "itemid": prod_resp_details["asset_id"],
                        "itemdetailsendpoint": prod_resp_details["asset_link"],
                        "itemcrawlstatus": prod_resp_details["status"],
                        "error": prod_resp_details["error"],
                        "meta": {
                            "platformassetid": prod_resp_details["asset_id"],
                            "pcid": prod_resp_details["pc_id"],
                            "itemname": prod_resp_details["name"],
                            "offerst": prod_resp_details["offering_start"],
                            "offerend": prod_resp_details["offering_end"]
                        }
                    }
                    prod_result_list.append(prod_result)


                while_loop_checker = False
            if while_loop_checker:
                page = False

        if error_count > 0 and asset_counter > 0:
            crawl_status = "partial"
        elif error_count == 0 and asset_counter > 0:
            crawl_status = "success"
        elif error_count == 0 and asset_counter == 0:
            crawl_status = 'error'
        elif error_count > 0 and asset_counter == 0:
            crawl_status = 'error'

        if prod_error_count > 0 and prod_asset_counter > 0:
            prod_crawl_status = "partial"
        elif prod_error_count == 0 and prod_asset_counter > 0:
            crawl_status = "success"
        elif prod_error_count == 0 and prod_asset_counter == 0:
            prod_crawl_status = 'error'
        elif prod_error_count > 0 and prod_asset_counter == 0:
            prod_crawl_status = 'error'

        end_crawler_resp = all_ingest_api.end_crawl(asset_counter, crawl_started, crawl_status, result_list, None,
                                                    error_count)

        prod_end_crawler_resp = production_ingest.end_crawl(prod_asset_counter, prod_crawl_started, prod_crawl_status,prod_result_list, None,prod_error_count)
        print("end_crawler_resp" , end_crawler_resp)
        print("asset count =" + str(asset_counter) + "and error count = " + str(error_count))
        # print(json.dumps(result_list))


    except Exception as e:

        print("error", e)

        all_ingest_api.end_crawl(asset_counter, crawl_started, "error", result_list, str(e), error_count)
        production_ingest.end_crawl(prod_asset_counter, prod_crawl_started, "error", prod_result_list, str(e),prod_error_count)

        # print(json.dumps(result_list))



def auction_asset_parser(asset_link, asset_id, lot_number  , opening_bid , asset_status,auction_name):
    sold_for = None
    current_bid = None
    image_list = []
    description = None
    pc_id = None
    title = None
    offering_result = "UNSOLD"
    environment = ["dev", "prod"]



    try:
        asset_session = requests.session()
        # print(asset_link)
        asset_resp = asset_session.get(asset_link)
        print(asset_resp)
        auction_details = BeautifulSoup(asset_resp.text, 'lxml')

        # date detailes
        if asset_resp.status_code == 200:
            auction_data = auction_details.find(class_='items').find("div", class_="sidebar-widget").p.text
            dates = " ".join(auction_data.split())
            dates = dates.split("End")
            start_date = dates[0].replace("Start: ","").replace("/","-").replace(" PM",":00").replace(" AM",":00")
            end_date = dates[1].replace(": ","").replace("/","-").replace(" PM",":00").replace(" AM",":00")
            # print(start_date+ "this")


            title = auction_details.find(class_="col-sm-9 col-sm-pull-3").h2.text
            title = " ".join(title.split()).split(":")[1]
            # print("title", title)

            all_details = auction_details.find(class_= 'col-md-7 col-sm-7').find_all(class_= "row")
            for ind , each_detail in enumerate(all_details):

                # print("========================================")
                # print(ind)
                # print(each_detail)

                if ind == 3:

                    bid_data = " ".join((each_detail.h3.text).split())
                    # print(bid_data)
                    if "SOLD" in bid_data:
                        sold_for = float(bid_data.split("$")[1].replace(",",""))
                        status = "OFFERING_CLOSED"
                        offering_result = "SOLD"
                        current_bid = sold_for
                    elif "CURRENT BID" in bid_data:
                        current_bid = float(bid_data.split("$")[1].replace(",",""))
                        status = "OFFERING_OPEN"

                if ind == 10:
                    description = each_detail.p.text.strip()
                    # print(description)

            if current_bid == 0:
                current_bid = opening_bid

            if description == "" or description == None:
                description = "null"

            if asset_status == "Open":
                status = "OFFERING_OPEN"
            elif asset_status == "Unsold":
                status = "OFFERING_CLOSED"
                offering_result = "UNSOLD"

            # print("status", status)

            image_details = auction_details.find(class_= 'col-md-5 col-sm-5').find_all("a")
            for each_image in image_details:
                # print(each_image["href"])
                imageUrl = {
                    "media_type": "image",
                    "media_src": each_image["href"],
                    "thumbnail": None,
                    "caption": None,
                    "is_active": True
                }
                image_list.append(imageUrl)

            # print(image_list)
            auction_start = str(datetime.datetime.strptime(start_date.replace(" EST",""), "%m-%d-%Y %H:%M:%S "))
            auction_end = str(datetime.datetime.strptime(end_date.replace(" EST", ""), "%m-%d-%Y %H:%M:%S"))

            end_utc_time = time.strptime(str(auction_end), "%Y-%m-%d %H:%M:%S")
            offering_end = timegm(end_utc_time)


            # print(auction_end , auction_start)
            asset = {
                "platform_asset_id": asset_id,
                "asset_type": "COLLECTIBLE",
                "pricing_type": "auction",
                "name": title,
                "description": description,
                "url": asset_link,
                "tags": None,
                "symbol": None,
                "current_bid": current_bid,
                "attributes": None,
                "currency_code": "USD",
                "status": status,
                "auction_start": auction_start + " EST",
                "auction_end": auction_end + " EST",
                "base_price": opening_bid,
                "bid_increment": None,
                "lot_id": lot_number,
                "lot_name": None,
                "auction_id": None,
                "auction_name": auction_name,
                "auction_type": None,
                "media": image_list,
                "custom_data": None,
                "reserve_price": None,
                "winning_bid": sold_for,
                "final_price": None,
                "offering_result": offering_result
            }
            # print(json.dumps(asset))



            for each in environment:
                if each == "dev":
                    try:

                        ####### ingest api call

                        pc_id = all_ingest_api.ingest_api(asset)
                        print(pc_id)

                        # print("current_bid" , current_bid)
                        all_ingest_api.asset_price_method(asset_id, current_bid, None)
                        print("success")

                        ################################################################

                        resp = {
                            "asset_link": asset_link,
                            "asset_id": asset_id,
                            "status": "success",
                            "error": None,
                            "pc_id": pc_id,
                            "name": title,
                            "offering_start": None,
                            "offering_end": offering_end
                        }

                    except Exception as e:
                        print("error in dev ingest", e)

                        resp = {
                            "asset_link": asset_link,
                            "asset_id": asset_id,
                            "status": "success",
                            "error": None,
                            "pc_id": pc_id,
                            "name": title,
                            "offering_start": None,
                            "offering_end": None
                        }


                elif each == "prod":
                    try:

                        ####### ingest api call

                        pc_id = production_ingest.ingest_api(asset)
                        print(pc_id)

                        # print("current_bid" , current_bid)
                        production_ingest.asset_price_method(asset_id, current_bid, None)
                        print("success")

                        ################################################################

                        prod_resp = {
                            "asset_link": asset_link,
                            "asset_id": asset_id,
                            "status": "success",
                            "error": None,
                            "pc_id": pc_id,
                            "name": title,
                            "offering_start": None,
                            "offering_end": offering_end
                        }

                    except Exception as e:
                        print("error in production ingest", e)

                        resp = {
                            "asset_link": asset_link,
                            "asset_id": asset_id,
                            "status": "success",
                            "error": None,
                            "pc_id": pc_id,
                            "name": title,
                            "offering_start": None,
                            "offering_end": None
                        }


            return resp, prod_resp

        
    except Exception as e:
        print( "error " , e)
        print(e)
        resp = {
            "asset_link": asset_link,
            "asset_id": asset_id,
            "status": "error",
            "error": None,
            "pc_id": pc_id,
            "name": title,
            "offering_start": None,
            "offering_end": None
        }

        prod_resp = {
            "asset_link": asset_link,
            "asset_id": asset_id,
            "status": "error",
            "error": None,
            "pc_id": pc_id,
            "name": title,
            "offering_start": None,
            "offering_end": None
        }

        return resp, prod_resp


if __name__ == '__main__':
    cc_auctions()





    # open_auction = auction_asset_parser("https://thecollectorconnection.com/bids/bidplace.aspx?itemid=41342", "41342" , "1" , "25","open" ,"Post War Sports Cards & Memorabilia XX1 Jan, 2023")
    # sold_auction , error_msg = auction_asset_parser("https://thecollectorconnection.com/bids/bidplace?itemid=40392", "40392" , "810",  None, None)
    # start_date = "11-27-2022 10:00:00 EST"
    # auction_start = datetime.datetime.strptime("2-5-2023 10:00:00 EST".replace(" EST", ""), "%m-%d-%Y %H:%M:%S")
    # print(auction_start)

