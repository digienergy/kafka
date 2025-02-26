from datetime import datetime


class CleanECU1051:

    def __init__(self,datalogger_brand):

        self.datalogger_brand = datalogger_brand
        
    def handler_time(self,year,month,date,hour,minute,second):
        timestamp = datetime(2000+int(year), int(month), int(date), int(hour), int(minute), int(second))
        return timestamp.strftime('%Y-%m-%d %H:%M:%S')

    def hex_to_ascii(self,num):
        return chr(int(hex(int(num))[2:][:2], 16)) + chr(int(hex(int(num))[2:][2:], 16))

    def handler_device_serial_number(self,num1,num2,num3,num4,num5,num6,num7,num8):

        serial_number = ""

        for num in [num1, num2, num3, num4, num5, num6, num7, num8]:
            serial_number += self.hex_to_ascii(num)

        return serial_number

    def handler_device_type(self,num1,num2,num3,num4,num5):
        device_type = ""

        for num in [num1, num2, num3, num4, num5]:
            device_type += self.hex_to_ascii(num)

        return device_type

    def message_to_dict(self,data):
        dict = {}
    
        for entry in data:
            if entry['tag'] == '#MOBILE_IP' :
                pass
            else:
                key= entry['tag'].split(':')[1].lower() 
                dict[key] = entry['value']
        collect_time = self.handler_time(dict.pop("year"),dict.pop("month"),dict.pop("date"),dict.pop("hour"),dict.pop("minute"),dict.pop("second"))
        dict["collecttime"] = collect_time
        
        serial_number = self.handler_device_serial_number(dict.pop("deviceserialnumber1"),dict.pop("deviceserialnumber2"),
                                    dict.pop("deviceserialnumber3"),dict.pop("deviceserialnumber4"),
                                    dict.pop("deviceserialnumber5"),dict.pop("deviceserialnumber6"),
                                    dict.pop("deviceserialnumber7"),dict.pop("deviceserialnumber8"))
        devicetype = self.handler_device_type(dict.pop("devicetype1"),dict.pop("devicetype2"),dict.pop("devicetype3"),
                                        dict.pop("devicetype4"),dict.pop("devicetype5"))
        
        dict["serialnumber"] = serial_number
        dict["devicetype"] = devicetype
        
        dict["gprsburninmode"] = dict["gprsburninmode"]
        dict.pop("gprsburninmode")

        return dict