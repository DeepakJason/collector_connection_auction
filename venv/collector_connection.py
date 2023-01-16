from bs4 import BeautifulSoup
import requests
import lxml
import datetime
import json
import ingest_api as all_ingest_api


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

    crawl_started = all_ingest_api.start_crawl()
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
                print(asset_id)
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

                resp_details , status_msg = auction_asset_parser(asset_link, asset_id, lot_number , opening_bid , status, auction_name)
                # print(resp_status)

                if status_msg == "success":
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


                while_loop_checker = False
            if while_loop_checker:
                page = False

        if error_count > 0 and asset_counter > 0:
            crawl_status = "partial"
        elif error_count == 0 and asset_counter > 0:
            crawl_status = "success"
        elif error_count == 0 and asset_counter == 0 or error_count > 0 and asset_counter == 0:
            crawl_status = 'error'

        end_crawler_resp = all_ingest_api.end_crawl(asset_counter, crawl_started, crawl_status, result_list, None, error_count)
        print("end_crawler_resp" , end_crawler_resp)
        print("asset count =" + str(asset_counter) + "and error count = " + str(error_count))
        # print(json.dumps(result_list))


    except Exception as e:

        print("error", e)

        all_ingest_api.end_crawl(asset_counter, crawl_started, "error", result_list, str(e), error_count)

        # print(json.dumps(result_list))

def auction_asset_parser(asset_link, asset_id, lot_number  , opening_bid , asset_status,auction_name):
    sold_for = None
    current_bid = None
    image_list = []
    description = None
    pc_id = None
    title = None
    offering_result = "UNSOLD"



    try:
        asset_session = requests.session()
        asset_resp = asset_session.get(asset_link)
        print(asset_resp)
        auction_details = BeautifulSoup(asset_resp.text, 'lxml')

        # date detailes
        if asset_resp.status_code == 200:
            try:
                auction_data = auction_details.find(class_='items').find("div", class_="sidebar-widget").p.text
                dates = " ".join(auction_data.split())
                dates = dates.split("End")
                start_date = dates[0].replace("Start: ","").replace("/","-").replace(" PM",":00").replace(" AM",":00")
                end_date = dates[1].replace(": ","").replace("/","-").replace(" PM",":00").replace(" AM",":00")
                # print(start_date , end_date)
        
        
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
                            current_bid = float(sold_for.replace(",",""))
                        elif "CURRENT BID" in bid_data:
                            current_bid = float(bid_data.split("$")[1].replace(",",""))
                            status = "OFFERING_OPEN"
        
                    if ind == 10:
                        description = each_detail.p.text.strip()
                        # print(description)
        
                if current_bid == 0:
                    current_bid = opening_bid
        
                if description == "":
                    description = "null"
        
                if asset_status == "Open":
                    status = "OFFERING_OPEN"
                elif asset_status == "Unsold":
                    status = "OFFERING_CLOSED"
                    offering_result = "UNSOLD"
        
                print("status", status)
        
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
        
                pc_id = all_ingest_api.ingest_api(asset)
                print(pc_id)
        
        
                # print("current_bid" , current_bid)
                all_ingest_api.asset_price_method( asset_id ,current_bid, None)
                print("success")
        
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
                # print(resp)
                return resp, "success"
            except:

                ingest_url = "https://provider-dev.api.pricingculture.com/asset/" + asset_id + "/canceloffering"
                print(ingest_url)
                ingest_header = {
                    "api-key": "b76dab34-7f87-43d7-aa2a-24e1fa9e9f6a",
                    "secret": "1jDhRVpprrlGCZStl6LS",
                    'content-Type': 'application/json'
                }

                api_resp = requests.patch(ingest_url, headers=ingest_header)
                print(api_resp.text)

                # resp_data = json.loads(api_resp.content)
                print("complete")
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
                # print(resp)
                return resp, "success"
            
        
    except Exception as e:
        print( "error " , e)
        resp = {
            "asset_link": asset_link,
            "asset_id": asset_id,
            "status": "error",
            "error": str(e),
            "pc_id": pc_id,
            "name": title,
            "offering_start": None,
            "offering_end": None
        }
        # print(resp)
        return resp, "error"



if __name__ == '__main__':
    cc_auctions()
    # open_auction = auction_asset_parser("https://thecollectorconnection.com/bids/bidplace?itemid=39583", "123" , "556" , "23")
    # sold_auction , error_msg = auction_asset_parser("https://thecollectorconnection.com/bids/bidplace?itemid=40392", "40392" , "810",  None, None)
    # start_date = "11-27-2022 10:00:00 EST"
    # auction_start = datetime.datetime.strptime(end_date.replace(" EST", ""), "%m-%d-%Y %H:%M:%S")
    # print(auction_start)

