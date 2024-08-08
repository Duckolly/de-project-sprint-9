import uuid
from datetime import datetime
from typing import Any, Dict, List
from lib.pg import PgConnect
from pydantic import BaseModel
from logging import Logger


class h_user(BaseModel):
    h_user_pk: uuid.UUID
    user_id: str
    load_dt: datetime
    load_src: str

class h_product(BaseModel):
    h_product_pk: uuid.UUID
    product_id: str
    load_dt: datetime
    load_src: str 

class h_category(BaseModel):
    h_category_pk : uuid.UUID
    category_name: str
    load_dt: datetime
    load_src: str

class h_restaurant(BaseModel):
    h_restaurant_pk : uuid.UUID
    restaurant_id: str
    load_dt : datetime
    load_src: str

class h_order(BaseModel):
    h_order_pk : uuid.UUID
    order_id : str
    order_dt : str   
    load_dt : datetime
    load_src : str

class s_order_status(BaseModel):
    h_order_pk : uuid.UUID
    status:str
    load_dt:datetime
    load_src:str
    hk_order_status_hashdiff:str   

class s_order_cost(BaseModel):
    h_order_pk : uuid.UUID
    cost:float
    payment:float
    load_dt:datetime
    load_src:str
    hk_order_cost_hashdiff:str 

class s_product_names(BaseModel):
    h_product_pk : uuid.UUID
    name:str
    load_dt:datetime
    load_src:str
    hk_product_names_hashdiff:str

class s_restaurant_names(BaseModel):
    h_restaurant_pk : uuid.UUID
    name:str
    load_dt:datetime
    load_src:str
    hk_restaurant_names_hashdiff:str

class s_user_names(BaseModel):
    h_user_pk: uuid.UUID
    username: str
    userlogin: str
    load_dt: datetime
    load_src: str
    hk_user_names_hashdiff  : str

class l_product_restaurant(BaseModel):
    hk_product_restaurant_pk : uuid.UUID
    h_restaurant_pk : uuid.UUID
    h_product_pk : uuid.UUID
    load_dt: datetime
    load_src: str

class l_order_product(BaseModel):
    hk_order_product_pk : uuid.UUID
    h_order_pk : uuid.UUID
    h_product_pk : uuid.UUID
    load_dt: datetime
    load_src: str
    

class l_order_user(BaseModel):
    hk_order_user_pk : uuid.UUID
    h_order_pk : uuid.UUID
    h_user_pk : uuid.UUID
    load_dt: datetime
    load_src: str

class l_product_category(BaseModel):
    hk_product_category_pk : uuid.UUID
    h_product_pk : uuid.UUID
    h_category_pk : uuid.UUID
    load_dt: datetime
    load_src: str
    



class OrderDdsBuilder:
    def __init__(self, dict: Dict) -> None:
        self._dict = dict
        self.source_system = "kafka-stream"
        self.order_ns_uuid = uuid.uuid4() 

    def _uuid(self, obj: Any) -> uuid.UUID:
        return uuid.uuid5(namespace=self.order_ns_uuid, name=str(obj))
    
    def h_user(self) -> h_user:
        user_id = str(self._dict['user']['id'])
        return h_user(
            h_user_pk=self._uuid(user_id),
            user_id=user_id,
            load_dt=datetime.utcnow(),
            load_src=self.source_system
        )
    
    def h_product(self) -> List[h_product]:
        products = []
        for prod_dict in self._dict['products']:
            prod_id = prod_dict['id']
            products.append(
                h_product(
                    h_product_pk=self._uuid(prod_id),
                    product_id=prod_id,
                    load_dt=datetime.utcnow(),
                    load_src=self.source_system 
                )
            )
        return products
     
    def h_restaurant(self) -> h_restaurant:
        restaurant_id = str(self._dict['restaurant']['id'])
        return h_restaurant(
            h_restaurant_pk=self._uuid(restaurant_id),
            restaurant_id=restaurant_id,
            name=self._dict['restaurant']['name'],
            load_dt=datetime.utcnow(),
            load_src=self.source_system
        )
    
    def h_order(self) -> h_order:
        order_id = str(self._dict['id'])
        return h_order(
            h_order_pk=self._uuid(order_id),
            order_id=order_id,
            order_dt=self._dict['date'],
            load_dt=datetime.utcnow(),
            load_src=self.source_system
        )

    def h_category(self) -> List[h_category]:
        categories = []
        for prod_dict in self._dict['products']:
            category_name = prod_dict["category"]
            categories.append(
                h_category(
                    h_category_pk=self._uuid(category_name),
                    category_name=category_name,
                    load_dt=datetime.utcnow(),
                    load_src=self.source_system                    
                )
            )
        return categories
    def s_order_status(self) -> s_order_status:
        return s_order_status(
            h_order_pk=self._uuid(self._dict["id"]),
            status=self._dict["status"],
            load_dt=datetime.utcnow(),
            load_src=self.source_system,
            hk_order_status_hashdiff=str(self._uuid([self._dict["id"],self._dict["status"]]))
        )
    
    def s_order_cost(self) -> s_order_cost:
        return s_order_cost(
            h_order_pk=self._uuid(self._dict["id"]),
            cost=self._dict["cost"],
            payment=self._dict["payment"],
            load_dt=datetime.utcnow(),
            load_src=self.source_system,
            hk_order_cost_hashdiff=str(self._uuid([self._dict["id"],self._dict["cost"], self._dict["payment"]]))
        )
    
    def s_product_names(self) -> List[s_product_names]:
        pnames = []
        for prod_dict in self._dict['products']:
            prod_id = prod_dict['id']
            pnames.append(
                s_product_names(
                    h_product_pk=self._uuid(prod_id),
                    name=prod_dict['name'],
                    load_dt=datetime.utcnow(),
                    load_src=self.source_system,
                    hk_product_names_hashdiff=str(self._uuid([ prod_id, prod_dict['name']]))
                )
            )
        return pnames

    def s_restaurant_names(self) -> s_restaurant_names:
        rest = self._dict["restaurant"]
        return s_restaurant_names(
            h_restaurant_pk=self._uuid(rest["id"]),
            name=rest["name"],
            load_dt=datetime.utcnow(),
            load_src=self.source_system,
            hk_restaurant_names_hashdiff=str(self._uuid([ rest["id"], rest["name"] ]))
        )

    def s_user_names(self) -> s_user_names:
        usr = self._dict["user"]
        return s_user_names(
            h_user_pk=self._uuid(usr["id"]),
            username=usr["name"],
            userlogin=usr["login"],
            load_dt=datetime.utcnow(),
            load_src=self.source_system,
            hk_user_names_hashdiff=str(self._uuid([ usr["id"], usr["name"], usr["login"]]))
        )
    
    def l_product_restaurant(self) -> List[l_product_restaurant]:
        h_restaurant_pk = self._uuid(self._dict["restaurant"]["id"])
        l_pr = []
        for prod_dict in self._dict['products']:
            prod_id = prod_dict['id']
            h_product_pk = self._uuid(prod_id)
            l_pr.append(
                l_product_restaurant(
                    hk_product_restaurant_pk=str(self._uuid([ h_product_pk, h_restaurant_pk ])),
                    h_product_pk=h_product_pk,
                    h_restaurant_pk = h_restaurant_pk,
                    load_dt=datetime.utcnow(),
                    load_src=self.source_system
                )
            )
        return l_pr

    def l_order_product(self) -> List[l_order_product]:
        h_order_pk = self._uuid(self._dict["id"])
        l_op = []
        for prod_dict in self._dict['products']:
            prod_id = prod_dict['id']
            h_product_pk = self._uuid(prod_id)
            l_op.append(
                l_order_product(
                    hk_order_product_pk=str(self._uuid([ h_product_pk, h_order_pk ])),
                    h_product_pk=h_product_pk,
                    h_order_pk = h_order_pk,
                    load_dt=datetime.utcnow(),
                    load_src=self.source_system
                )
            )
        return l_op

    def l_order_user(self) -> l_order_user:
        h_order_pk = self._uuid(self._dict["id"])
        h_user_pk = self._uuid(self._dict["user"]["id"])
        return l_order_user(
            hk_order_user_pk = str(self._uuid([h_order_pk, h_user_pk])),
            h_order_pk = h_order_pk,
            h_user_pk = h_user_pk,
            load_dt=datetime.utcnow(),
            load_src=self.source_system
        )

    def l_product_category(self) -> List[l_product_category]:
        l_pc = []
        for prod_dict in self._dict['products']:
            prod_id = prod_dict['id']
            h_product_pk = self._uuid(prod_id)
            h_category_pk = self._uuid(prod_dict['category'])
            l_pc.append(
                l_product_category(
                    hk_product_category_pk=str(self._uuid([ h_product_pk, h_category_pk ])),
                    h_product_pk=h_product_pk,
                    h_category_pk = h_category_pk,
                    load_dt=datetime.utcnow(),
                    load_src=self.source_system
                )
            )
        return l_pc
    
    def cdm_product_msg(self) -> List[Dict]:
        msg = []
        for prod_dict in self._dict['products']:
            prd_msg = {}
            prd_msg["object_id"] = self._dict["id"]
            prd_msg["object_type"] = 'user_prod'
            pl = {}
            pl["user_id"] = self.h_user().user_id
            pl["product_id"] = prod_dict['id']
            pl["product_name"] = prod_dict["name"]
            pl["order_cnt"] = 1
            prd_msg["payload"] = pl
            msg.append(prd_msg)
        return msg
    
    def cdm_category_msg(self) -> List[Dict]:
        msg = []
        for prod_dict in self._dict['products']:
            prd_msg = {}
            prd_msg["object_id"] = str(self._dict["id"])
            prd_msg["object_type"] = 'user_categ'
            pl = {}
            pl["user_id"] = str(self.h_user().user_id)
            pl["category_id"] = str(self._uuid(prod_dict["category"]))
            pl["category_name"] = prod_dict["category"]
            pl["order_cnt"] = 1
            prd_msg["payload"] = pl
            msg.append(prd_msg)
        return msg

class DdsRepository:
    def __init__(self, db: PgConnect) -> None:
        self._db = db

    def h_user_insert(self, user: h_user) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.h_user(
                            h_user_pk,
                            user_id,
                            load_dt,
                            load_src
                        )
                        VALUES(
                            %(h_user_pk)s,
                            %(user_id)s,
                            %(load_dt)s,
                            %(load_src)s
                        )
                        ON CONFLICT (h_user_pk) DO NOTHING;
                    """,
                    {
                        'h_user_pk': user.h_user_pk,
                        'user_id': user.user_id,
                        'load_dt': user.load_dt,
                        'load_src': user.load_src
                    }
                )

    def s_user_names_insert(self, user: s_user_names) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO dds.s_user_names (
                        hk_user_names_hashdiff,
                        h_user_pk,
                        username,
                        userlogin,
                        load_dt,
                        load_src
                    )
                    VALUES (
                        %(hk_user_names_hashdiff)s,
                        %(h_user_pk)s,
                        %(username)s,
                        %(userlogin)s,
                        %(load_dt)s,
                        %(load_src)s
                    )
                    ON CONFLICT (h_user_pk) DO NOTHING;
                """, {
                    "hk_user_names_hashdiff": user.hk_user_names_hashdiff,
                    "h_user_pk": user.h_user_pk,
                    "username": user.username,
                    "userlogin": user.userlogin,
                    "load_dt": user.load_dt,
                    "load_src": user.load_src
                })

        
    
    def h_order_insert(self, obj: h_order) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.h_order(
                            h_order_pk,
                            order_id,
                            order_dt,
                            load_dt,
                            load_src
                        )
                        VALUES(
                            %(h_order_pk)s,
                            %(order_id)s,
                            %(order_dt)s,
                            %(load_dt)s,
                            %(load_src)s
                        )
                        ON CONFLICT (h_order_pk) DO NOTHING;
                    """,
                    {
                        'h_order_pk': obj.h_order_pk,
                        'order_id': obj.order_id,
                        'order_dt': obj.order_dt,
                        'load_dt': obj.load_dt,
                        'load_src': obj.load_src
                    }
                ) 

    def s_order_cost_insert(self, obj: s_order_cost) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.s_order_cost(
                            h_order_pk,
                            cost,
                            payment,
                            hk_order_cost_hashdiff,
                            load_dt,
                            load_src
                        )
                        VALUES(
                            %(h_order_pk)s,
                            %(cost)s,
                            %(payment)s,
                            %(hk_order_cost_hashdiff)s,
                            %(load_dt)s,
                            %(load_src)s
                        )
                        ON CONFLICT (h_order_pk, load_dt) DO NOTHING;
                    """,
                    {
                        'h_order_pk': obj.h_order_pk,
                        'cost': obj.cost,
                        'payment': obj.payment,
                        'hk_order_cost_hashdiff': obj.hk_order_cost_hashdiff,
                        'load_dt': obj.load_dt,
                        'load_src': obj.load_src
                    }
                ) 


    def s_order_status_insert(self, obj: s_order_status) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.s_order_status(
                            h_order_pk,
                            status,
                            hk_order_status_hashdiff,
                            load_dt,
                            load_src
                        )
                        VALUES(
                            %(h_order_pk)s,
                            %(status)s,
                            %(hk_order_status_hashdiff)s,
                            %(load_dt)s,
                            %(load_src)s
                        )
                        ON CONFLICT (h_order_pk, load_dt) DO NOTHING;
                    """,
                    {
                        'h_order_pk': obj.h_order_pk,
                        'status': obj.status,
                        'hk_order_status_hashdiff': obj.hk_order_status_hashdiff,
                        'load_dt': obj.load_dt,
                        'load_src': obj.load_src
                    }
                ) 

    def l_order_user_insert(self, obj: l_order_user) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.l_order_user(
                            hk_order_user_pk,
                            h_order_pk,
                            h_user_pk,
                            load_dt,
                            load_src
                        )
                        VALUES(
                            %(hk_order_user_pk)s,
                            %(h_order_pk)s,
                            %(h_user_pk)s,
                            %(load_dt)s,
                            %(load_src)s
                        )
                        ON CONFLICT (hk_order_user_pk) DO NOTHING;
                    """,
                    {
                        'hk_order_user_pk': obj.hk_order_user_pk,
                        'h_order_pk': obj.h_order_pk,
                        'h_user_pk': obj.h_user_pk,
                        'load_dt': obj.load_dt,
                        'load_src': obj.load_src
                    }
                )


    def h_product_insert(self, obj: List[h_product]) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                for each in obj:
                    cur.execute(
                        """
                            INSERT INTO dds.h_product(
                                h_product_pk,
                                product_id,
                                load_dt,
                                load_src
                            )
                            VALUES(
                                %(h_product_pk)s,
                                %(product_id)s,
                                %(load_dt)s,
                                %(load_src)s
                            )
                            ON CONFLICT (h_product_pk) DO NOTHING;
                        """,
                        {
                            'h_product_pk': each.h_product_pk,
                            'product_id': each.product_id,
                            'load_dt': each.load_dt,
                            'load_src': each.load_src
                        }
                    ) 

    def s_product_names_insert(self, obj: List[s_product_names]) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                for each in obj:
                    cur.execute(
                        """
                            INSERT INTO dds.s_product_names(
                                h_product_pk,
                                name,
                                load_dt,
                                load_src,
                                hk_product_names_hashdiff
                            )
                            VALUES(
                                %(h_product_pk)s,
                                %(name)s,
                                %(load_dt)s,
                                %(load_src)s,
                                %(hk_product_names_hashdiff)s
                            )
                            ON CONFLICT (h_product_pk) DO NOTHING;
                        """,
                        {
                            'h_product_pk': each.h_product_pk,
                            'name': each.name,
                            'load_dt': each.load_dt,
                            'load_src': each.load_src,
                            'hk_product_names_hashdiff': each.hk_product_names_hashdiff
                        }
                    ) 


    def l_order_product_insert(self, obj: l_order_product) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                for each in obj:
                    cur.execute(
                        """
                            INSERT INTO dds.l_order_product(
                                hk_order_product_pk,
                                h_order_pk,
                                h_product_pk,
                                load_dt,
                                load_src
                            )
                            VALUES(
                                %(hk_order_product_pk)s,
                                %(h_order_pk)s,
                                %(h_product_pk)s,
                                %(load_dt)s,
                                %(load_src)s
                            )
                            ON CONFLICT (hk_order_product_pk) DO NOTHING;
                        """,
                        {
                            'hk_order_product_pk': each.hk_order_product_pk,
                            'h_order_pk': each.h_order_pk,
                            'h_product_pk': each.h_product_pk,
                            'load_dt': each.load_dt,
                            'load_src': each.load_src
                        }
                    )

    def h_restaurant_insert(self, obj: h_restaurant) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.h_restaurant(
                            h_restaurant_pk,
                            restaurant_id,
                            load_dt,
                            load_src
                        )
                        VALUES(
                            %(h_restaurant_pk)s,
                            %(restaurant_id)s,
                            %(load_dt)s,
                            %(load_src)s
                        )
                        ON CONFLICT (h_restaurant_pk) DO NOTHING;
                    """,
                    {
                        'h_restaurant_pk': obj.h_restaurant_pk,
                        'restaurant_id': obj.restaurant_id,
                        'load_dt': obj.load_dt,
                        'load_src': obj.load_src
                    }
                ) 

    def s_restaurant_names_insert(self, obj: s_restaurant_names) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.s_restaurant_names(
                            h_restaurant_pk,
                            name,
                            load_dt,
                            load_src,
                            hk_restaurant_names_hashdiff
                        )
                        VALUES(
                            %(h_restaurant_pk)s,
                            %(name)s,
                            %(load_dt)s,
                            %(load_src)s,
                            %(hk_restaurant_names_hashdiff)s
                        )
                        ON CONFLICT (h_restaurant_pk) DO NOTHING;
                    """,
                    {
                        'h_restaurant_pk': obj.h_restaurant_pk,
                        'name': obj.name,
                        'load_dt': obj.load_dt,
                        'load_src': obj.load_src,
                        'hk_restaurant_names_hashdiff': obj.hk_restaurant_names_hashdiff
                    }
                ) 



    def l_product_restaurant_insert(self, obj: l_product_restaurant) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                for each in obj:
                    cur.execute(
                        """
                            INSERT INTO dds.l_product_restaurant(
                                hk_product_restaurant_pk,
                                h_restaurant_pk,
                                h_product_pk,
                                load_dt,
                                load_src
                            )
                            VALUES(
                                %(hk_product_restaurant_pk)s,
                                %(h_restaurant_pk)s,
                                %(h_product_pk)s,
                                %(load_dt)s,
                                %(load_src)s
                            )
                            ON CONFLICT (hk_product_restaurant_pk) DO NOTHING;
                        """,
                        {
                            'hk_product_restaurant_pk': each.hk_product_restaurant_pk,
                            'h_restaurant_pk': each.h_restaurant_pk,
                            'h_product_pk': each.h_product_pk,
                            'load_dt': each.load_dt,
                            'load_src': each.load_src
                        }
                    )

    def h_category_insert(self, obj: List[h_category]) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                for each in obj:
                    cur.execute(
                        """
                            INSERT INTO dds.h_category(
                                h_category_pk,
                                category_name,
                                load_dt,
                                load_src
                            )
                            VALUES(
                                %(h_category_pk)s,
                                %(category_name)s,
                                %(load_dt)s,
                                %(load_src)s
                            )
                            ON CONFLICT (h_category_pk) DO NOTHING;
                        """,
                        {
                            'h_category_pk': each.h_category_pk,
                            'category_name': each.category_name,
                            'load_dt': each.load_dt,
                            'load_src': each.load_src
                        }
                    ) 

    def l_product_category_insert(self, obj: l_product_category) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                for each in obj:
                    cur.execute(
                        """
                            INSERT INTO dds.l_product_category(
                                hk_product_category_pk,
                                h_category_pk,
                                h_product_pk,
                                load_dt,
                                load_src
                            )
                            VALUES(
                                %(hk_product_category_pk)s,
                                %(h_category_pk)s,
                                %(h_product_pk)s,
                                %(load_dt)s,
                                %(load_src)s
                            )
                            ON CONFLICT (hk_product_category_pk) DO NOTHING;
                        """,
                        {
                            'hk_product_category_pk': each.hk_product_category_pk,
                            'h_category_pk': each.h_category_pk,
                            'h_product_pk': each.h_product_pk,
                            'load_dt': each.load_dt,
                            'load_src': each.load_src
                        }
                    )