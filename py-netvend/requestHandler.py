import pickle, netvend

class NetvendRequest:
    def __init__(self, from_address, value, tip_ts, post_address, data, post_ts):
        self.from_address = from_address
        self.value = value
        self.tip_ts = tip_ts
        self.post_address = post_address
        self.data = data
        self.post_ts = post_ts


class RequestHandler:
    def __init__(self, nv, loadName=None):
        if loadName==None:
            self.nv = nv
            self.lastTipID = 0
            self.tipThreshold = 0
        else:
            self.load(loadName)

    def save(self, name):
        dict = self.__dict__
        nv = self.nv
        dict['nv'] = None
        f = open(name+'.rh', 'wb')
        pickle.dump(dict, f)
        f.close()
        self.nv = nv

    def load(self, name, nv):
        f = open(name+'.rh', 'rb')
        dict = pickle.load(f)
        self.__dict__.update(dict)
        self.nv = nv
        f.close()

    def setTipThreshold(self, uSats):
        self.tipThreshold = uSats

    def getNewRequests(self):
        requests = []

        query = "SELECT " \
                    "tips.tip_id, " \
                    "tips.from_address, " \
                    "tips.value, " \
                    "tips.post_id, " \
                    "tips.ts, " \
                    "posts.address, " \
                    "posts.data, " \
                    "posts.ts " \
                "FROM tips LEFT JOIN posts " \
                "ON tips.post_id = posts.post_id " \
                "WHERE " \
                        "tips.to_address = '" + self.nv.get_address() + "' " \
                    "AND tips.tip_id > " + str(self.lastTipID) + " " \
                "ORDER BY tip_id ASC"

        response = self.nv.query(query, 4000)
        if not response['success']:
            raise netvend.NetvendResponseError(response)

        rows = response['command_result']['rows']
        for row in rows:
            tip_id, from_address, tip_value, post_id, tip_ts, post_address, data, post_ts = row

            self.lastTipID = tip_id

            if tip_value >= self.tipThreshold:
                requests.append(NetvendRequest(from_address, tip_value, tip_ts, post_address, data, post_ts))

        return requests