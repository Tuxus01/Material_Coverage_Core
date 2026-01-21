"""
Material Coverage - Core
Developer : Christian Valladares

"""
import pyodbc
import pandas as pd 
import numpy as np
import matplotlib.pyplot as plt
import datetime

conexionDBPolyPM =  r'DRIVER={SQL Server};SERVER=;DATABASE=;UID=;PWD='


#########################################
#               Querys
#     Donwload data SQL server PolyPM
#########################################
def Generate_query_result(SQL):
    try:
        with pyodbc.connect(conexionDBPolyPM) as cnxn:
            with cnxn.cursor() as cursor:
                cursor.execute(SQL)
                rows = cursor.fetchall()
                nombres_campos = [column[0] for column in cursor.description]
                datos = [dict(zip(nombres_campos, fila)) for fila in rows]
        return pd.DataFrame(datos)
    except pyodbc.Error as e:
        print("Error connecting to the database.: ", e)
        return []
    finally:
        if cursor:
            cursor.close()
        if cnxn:
            cnxn.close()

def helper_POs(PO):
    # Verifica y calcula los deliveries, de no existir crea un scheduled de proyeccion!
    # Elimina los envios ya recibido y recalcula los pendientes
    try:
        POx = PO.loc[ PO['Open'] > 0, ['PO', 'RawMaterialID','Open', 'Ordered', 'Received', 'ETAPO']].drop_duplicates()
        delivery_sum = PO.loc[(PO['Delivery'] > 0) & (PO['Status'] != 'Complete')].groupby(['PO', 'RawMaterialID'])['Delivery'].sum().reset_index()
        POx = POx.merge(delivery_sum, on=['PO', 'RawMaterialID'], how='left')
        POx['Delivery'] = POx['Delivery'].fillna(0)
        POx['Delivery_n'] = POx['Open'] - POx['Delivery']
        Encotrado = False
        ConT = 0 
        ConF = 0
        for _, i in POx.loc[(POx['Delivery_n'] > 0 ) & ( ((POx['Delivery_n'] / POx['Ordered']) * 100) >= 5 )].iterrows():
            for x,y in PO.loc[(PO['PO'] == POx.at[_,'PO']) & (PO['RawMaterialID'] == POx.at[_,'RawMaterialID'])  & (PO['Delivery'] == 0)].iterrows():
                Encotrado = True 
                PO.at[x,'PO'] += ' (Simulation)'
                PO.at[x,'Delivery'] = float(POx.at[_,'Delivery_n']) * float(PO.at[x,'ConversionFactor'])
                PO.at[x,'ETA'] = POx.at[_,'ETAPO']
                PO.at[x,'Status'] = 'OnTime'
                break 
            if Encotrado == True:
                ConT+=1
            else:
                ConF+=1
                mask = (PO['PO'] == POx.loc[_,'PO']) & (PO['RawMaterialID'] == POx.loc[_,'RawMaterialID'])
                PO.loc[mask, 'Delivery'] = float(POx.loc[_,'Delivery_n']) * PO.loc[mask, 'ConversionFactor']
                PO.loc[mask, 'ETA'] = POx.loc[_,'ETAPO']
                PO.loc[mask, 'Status'] = 'OnTime'
            Encotrado = False
        return PO
    except:
        print('Error in DataFrame')
        return PO 

def helper_Pivots(mos):

    # =========================
    #  Material Needed (Problemas)
    # =========================
    PV_MaterialNeeded = (
        mos.loc[mos['StatusPN'] == 'Material Needed']
        .pivot_table(
            index='Category',
            columns='Customer', 
            values='Pending', 
            aggfunc='sum', 
            fill_value=0)
    )
    print(PV_MaterialNeeded)
    # Material Needed por PartNumber
    PV_MaterialNeeded_PN = (
        mos.loc[mos['StatusPN'] == 'Material Needed']
        .pivot_table(
            index='Category',
            columns='Customer',
            values='Pending',
            aggfunc='sum',
            fill_value=0
        )
    )
    print(PV_MaterialNeeded_PN)
    ## Material 
    mos['DaysToPRD'] = (mos['PRD'] - pd.Timestamp.today()).dt.days
    PV_Aging = (
        mos.loc[mos['StatusPN'] == 'Material Needed']
        .pivot_table(
            index=pd.cut(
                mos['DaysToPRD'],
                bins=[-999, 0, 7, 14, 30, 999],
                labels=['Past Due', '0-7', '8-14', '15-30', '30+']
            ),
            values='Pending',
            aggfunc='sum',
            fill_value=0
        )
    )
    print(PV_Aging)
    # =========================
    # Transferencias de materiales
    # =========================
    # Onhand por transferir
    PV_Transfer = (
        mos.loc[mos['Status'] == 'Cover (Transfer)']
        .pivot_table(
            index='Category',
            columns='SubCategory',
            values='Pending_static',
            aggfunc='sum',
            fill_value=0
        )
    )
    # =========================
    # Riesgos por POs Tarde
    # =========================
    PV_PO_Risk = mos.loc[
        mos['Status'].isin(['Out of ETA'])
    ].pivot_table(
        index='Vendor',
        columns='StatusPN',
        values='Delivery_Balance',
        aggfunc='sum',
        fill_value=0
    )
    # =========================
    #  por POs OnTime
    # =========================
    PV_PO_Category = mos.loc[
        mos['Status'].isin(['Cover with PO'])
    ].pivot_table(
        index='Category',
        columns='StatusPN',
        values='Delivery_Balance',
        aggfunc='sum',
        fill_value=0
    )

    
    # -----------------------------------
    # PRD, StatusPN, Pending_static - YEAR_WEEK resumen
    # -----------------------------------

    mos['PRD'] = pd.to_datetime(mos['PRD'])
    Today = pd.Timestamp.today().normalize()  
    mos_future = mos.loc[(mos['PRD'] >= Today) & (~mos['StatusPN'].isin(['Complete','Overconsumption']))].copy()
    mos_future['YearWeek'] = mos_future['PRD'].dt.strftime('%Y-%U')  
    pivot_graph = (
        mos_future.groupby(['YearWeek', 'StatusPN', 'Category'])['Pending_static']
        .sum()
        .reset_index()
    )
    pivot_graph_table = pivot_graph.pivot(
        index=['YearWeek','Category'],
        columns='StatusPN',
        values='Pending_static'
    ).fillna(0)

    print(pivot_graph_table)
    

    return PV_MaterialNeeded,PV_MaterialNeeded_PN,PV_Aging,PV_Transfer,PV_PO_Risk,PV_PO_Category, pivot_graph_table


headers = [
    "SerialNumber",     # ID MO
    "Customer",         # Cliente
    "StatusNameMO",     # Estatus MO
    "TypeMO",           # Tipo MO
    "MO",               # Manufacture Order
    "CutNumber",        # Numero de Corte
    "GoodWarehouse",    # Bodega de Manufactura  
    "Warehouse",        # Bodega Interna 
    "PRD",              # Material Date / Fecha Requemiento
    "Make",             # Cantidad de Piezas
    "PreferentVendor",  # Proveedor por Temporada
    "PartNumber",       # Numero de Parte
    "ComponentName",    # Componente
    "Category",         # Categoria - Fabri, Trim, Supplies, etc..
    "SubCategory",      # Sub Categoria
    "Required",         # Cantidad Requerida por la MO
    "Adjust",           # Ajustes
    "Withdrawn",        # Rebajas
    "Pending",          # Pending WithDranw
    "Pending_static",   # Pending WithDranw sin modificar
    "Inspect",          # Alerta de Cobertuar con material en estatus Inspect
    "Status",           # Resultado final de cada PN por la cobertura
    "StatusPN",         # Resultado final por PartNumber - (Generalizado)
    "ETA",              # Delivery Scheduled Date
    "PO",               # Order de Compra
    "UnitCost",         # Costo de compra del material
    "Vendor",           # Proveedor al que se le compro ese material
    "PurchasAgent",     # Comprador
    "StockWarehousePO", # Bodega al que llegara la PO
    "PurchaseTypeName", # Tipo de Compra
    "WH",               # Bodega de Transferencia
    "Delivery_Balance", # Cantida del delivery utilizad
    "Log",              # Logica de cobertura -- pasos que siguio el analisis
]


SQL_Inventory = f"""
    SELECT DISTINCT
        WH.WarehouseName as Warehouse,
        RM.PartNumber,
        COMPL.ComponentName,
        COMPC.CategoryName as Category,
        COMPS.SubCategoryName as SubCategory,
        ISNULL(SL.StockOnHand,0) as OnHand,
        ISNULL(SL.StockOrdered,0) as Ordered,
        ISNULL(SL.StockInspect,0) as Inspect,
        ISNULL(SL.StockRejected,0) as Rejected,
        0 as Assigned,
        ISNULL((Select Sum(QuantityRequired+IsNull(QuantityAdjust, 0)-QuantityWithdrawn) From RawAllocations Left Outer Join ManufactureOrders  On ManufactureOrders.ManufactureID=RawAllocations.ManufactureID  Where StockLocationID=SL.StockLocationID   And (StatusID>=40 And StatusID<90 And BlanketOrderID Is Null) And QuantityRequired+IsNull(QuantityAdjust, 0)>QuantityWithdrawn),0) as Allocated,
        ISNULL((Select Sum(QuantityRequired+IsNull(QuantityAdjust, 0)-QuantityWithdrawn) From RawAllocations Left Outer Join ManufactureOrders  On ManufactureOrders.ManufactureID=RawAllocations.ManufactureID  Where StockLocationID=SL.StockLocationID   And (StatusID=20 And BlanketOrderID Is Null) And QuantityRequired+IsNull(QuantityAdjust, 0)>QuantityWithdrawn),0) as Forecast,
        RM.RawMaterialID,
        WH.WarehouseID

    FROM StockLocation as SL WITH(NOLOCK)
        LEFT OUTER JOIN RawMaterials as RM WITH(NOLOCK) ON RM.RawMaterialID=SL.RawMaterialID
        LEFT OUTER JOIN Warehouses as WH WITH(NOLOCK) ON WH.WarehouseID=SL.StockWarehouseID 
        LEFT OUTER JOIN ComponentLibrary as COMPL WITH(NOLOCK) ON COMPL.ComponentID=RM.ComponentID
        LEFT OUTER JOIN ComponentCategories as COMPC WITH(NOLOCK) ON COMPC.ComponentCategoryID=COMPL.ComponentCategoryID
        LEFT OUTER JOIN ComponentSubcategories as COMPS WITH(NOLOCK) ON COMPS.SubCategoryID=COMPL.SubCategoryID
        LEFT OUTER JOIN Colors as COLOR WITH(NOLOCK) ON COLOR.ColorID=RM.ColorID

    WHERE
        COMPC.CategoryName in ('Fabric','Trim')
        AND SL.StockOnHand+SL.StockOrdered+SL.StockInspect+SL.StockRejected > 0
    """

SQL_Purchase_Orders = f"""
    SELECT 
        PAgent.CompanyNumber as Agent,
        PO.PONumber as PO,
        POT.PurchaseTypeName,
        C.CompanyNumber as Customer,
        V.CompanyNumber as Vendor,
        W.WarehouseName as Warehouse,
        PO.StockWarehouseID as WarehouseID,
        STPO.StatusName as 'Status',
        PO.OrderDate,
        PO.ScheduleDate as ETAPO,
        PO.SubTotal,
        --Detail Purchase Orders
        POD.ItemNumber as Item,
        COMPL.ComponentName as Component,
        COMPC.CategoryName as Category,
        COMPS.SubCategoryName as SubCategory,
        RM.RawMaterialID,
        RM.PartNumber,
        POD.QuantityOrdered as Ordered,
        POD.QuantityReceived as Received,
        POD.QuantityRejected as Rejected,
        (POD.QuantityOrdered - POD.QuantityReceived) as 'Open',
        POD.UnitPrice,
        ISNULL(PODV.ScheduledDeliveryDate,PO.ScheduleDate) as ETA, 
        -- Calculos
        (CASE WHEN 
            ISNULL(PODVD.DeliveryQuantity,0) > 0 
            and (
                        SELECT SUM(PODVDx.DeliveryQuantity)
                        FROM PODeliveries as PODVx WITH(NOLOCK) 
                        LEFT OUTER JOIN PODeliveryDetails as PODVDx WITH(NOLOCK) ON PODVDx.PODeliveryID = PODVx.PODeliveryID 
                        WHERE
                        PODVx.PurchaseID=PO.PurchaseID 
                        and PODVDx.PurchaseDetailID = POD.PurchaseDetailID
                        AND PODVx.ScheduledDeliveryDate <= PODV.ScheduledDeliveryDate
                        AND  ISNULL(PODVDx.DeliveryQuantity,0)  > 0
                    ) <= POD.QuantityReceived + 1
        THEN 
                'Complete'
        ELSE    
                --Validamos OnTime Late
            (
                CASE WHEN  ISNULL(PODVD.DeliveryQuantity,0) > 0 
                THEN 
                    (
                        CASE WHEN PODV.ScheduledDeliveryDate >= GETDATE() 
                        THEN 'OnTime' ELSE 'Late' END
                    )
                ELSE 'Complete'
                END
            )
        END)  as 'Status',
        -- Delivery Calculo    
        (
            CASE WHEN 
            (
                SELECT COUNT(*) FROM PODeliveries as PODVx WITH(NOLOCK) 
                LEFT OUTER JOIN PODeliveryDetails as PODVDx WITH(NOLOCK) ON PODVDx.PODeliveryID = PODVx.PODeliveryID 
                WHERE
                PODVx.PurchaseID=PO.PurchaseID 
                and PODVDx.PurchaseDetailID = POD.PurchaseDetailID
                AND  ISNULL(PODVDx.DeliveryQuantity,0)  > 0
            ) = (
                SELECT COUNT(*) FROM PODeliveries as PODVxx WITH(NOLOCK) 
                LEFT OUTER JOIN PODeliveryDetails as PODVDxx WITH(NOLOCK) ON PODVDxx.PODeliveryID = PODVxx.PODeliveryID 
                WHERE
                PODVxx.PurchaseID=PO.PurchaseID 
                and PODVDxx.PurchaseDetailID = POD.PurchaseDetailID
                AND PODVxx.ScheduledDeliveryDate <= PODV.ScheduledDeliveryDate
                AND  ISNULL(PODVDxx.DeliveryQuantity,0)  > 0
            ) and ISNULL(PODVD.DeliveryQuantity,0) > (POD.QuantityOrdered - POD.QuantityReceived)
            THEN 
                (POD.QuantityOrdered - POD.QuantityReceived)
            ELSE
                ISNULL(PODVD.DeliveryQuantity,0)
            END
        ) Delivery, 
        (CASE WHEN COMPL.ConversionFactor = 0 THEN 1 ELSE ISNULL(COMPL.ConversionFactor,1) END) as ConversionFactor
    FROM PurchaseOrders as PO WITH(NOLOCK)
        LEFT OUTER JOIN StatusNames as STPO WITH(NOLOCK) ON STPO.StatusID=PO.StatusID
        LEFT OUTER JOIN PurchaseDetails as POD WITH(NOLOCK) ON POD.PurchaseID=PO.PurchaseID
        LEFT OUTER JOIN RawMaterials as RM WITH(NOLOCK) ON RM.RawMaterialID=POD.RawMaterialID
        LEFT OUTER JOIN Addresses as PAgent WITH(NOLOCK) ON PAgent.AddressID=PO.PurchaseAgentID
        LEFT OUTER JOIN PurchaseTypes as POT WITH(NOLOCK) ON POT.PurchaseTypeID=PO.PurchaseTypeID
        LEFT OUTER JOIN ComponentLibrary as COMPL WITH(NOLOCK) ON COMPL.ComponentID=RM.ComponentID
        LEFT OUTER JOIN ComponentCategories as COMPC WITH(NOLOCK) ON COMPC.ComponentCategoryID=COMPL.ComponentCategoryID
        LEFT OUTER JOIN ComponentSubcategories as COMPS WITH(NOLOCK) ON COMPS.SubCategoryID=COMPL.SubCategoryID
        --Schedule.
        LEFT OUTER JOIN PODeliveries as PODV WITH(NOLOCK) ON PODV.PurchaseID=PO.PurchaseID 
        LEFT OUTER JOIN PODeliveryDetails as PODVD WITH(NOLOCK) ON PODVD.PODeliveryID = PODV.PODeliveryID and PODVD.PurchaseDetailID = POD.PurchaseDetailID 
        LEFT OUTER JOIN Addresses as C WITH(NOLOCK) ON C.AddressID = PO.CustomerID
        LEFT OUTER JOIN Addresses as V WITH(NOLOCK) ON V.AddressID = PO.VendorID
        LEFT OUTER JOIN Warehouses as W WITH(NOLOCK) ON W.WarehouseID = PO.StockWarehouseID
    WHERE
        STPO.StatusName in ('Issued')
        And POD.QuantityOrdered > 0
        And COMPC.CategoryName in ('Fabric','Trim')
        

    ORDER BY
        PO.OrderDate,
        PAgent.CompanyNumber,
        PO.PONumber,
        POD.ItemNumber;

"""

SQL_ManufactureOrders = f"""
    SELECT DISTINCT
        STMO.StatusName as StatusNameMO,
        MO.ManufactureID as SerialNumber,
        MO.ManufactureNumber as MO,
        MO.CutNumber,
        GodWarehouse.WarehouseName as GoodWarehouse,
        GodWarehouse.WarehouseID as WarehouseID,
        (CASE WHEN RA.UseTrimWarehouse = 0 THEN  STOCK.WarehouseName ELSE TRIMS.WarehouseName END ) as Warehouse,
        (CASE WHEN RA.UseTrimWarehouse = 0 THEN  STOCK.WarehouseID ELSE TRIMS.WarehouseID END ) as StockWarehouseID,
        MO.MaterialDate as PRD,
        MO.QuantityOrdered as Make,
        RM.PartNumber,
        COMPL.ComponentName,
        COMPC.CategoryName as Category,
        COMPS.SubCategoryName as SubCategory,
        RA.QuantityRequired as Required,
        ISNULL(RA.QuantityAdjust,0) as Adjust,
        ISNULL(RA.QuantityWithdrawn,0) as Withdrawn,
        (RA.QuantityRequired + ISNULL(RA.QuantityAdjust,0) - ISNULL(RA.QuantityWithdrawn,0)) as Pending,
        (RA.QuantityRequired + ISNULL(RA.QuantityAdjust,0) - ISNULL(RA.QuantityWithdrawn,0)) as Pending_static,
        (CASE WHEN (RA.QuantityRequired + ISNULL(RA.QuantityAdjust,0) - ISNULL(RA.QuantityWithdrawn,0)) > 0 THEN 'Pending' WHEN (RA.QuantityRequired + ISNULL(RA.QuantityAdjust,0) - ISNULL(RA.QuantityWithdrawn,0)) < 0 THEN 'Overconsumption' ELSE 'Complete' END) as Status,
        ISNULL(ADSCUS.CompanyNumber,'Dummies') as Customer,
        DW.EnumValue as TypeMO,
        RM.RawMaterialID,
        '' as Log,
        --Campos Relleno
        '' as ETA,
        '' as PO,
        '' as Vendor,
        '' as PurchasAgent,
        '' as StockWarehousePO,
        '' as PurchaseTypeName,
        '' as WH,
        0 as Delivery_Balance
    FROM ManufactureOrders as MO WITH(NOLOCK)
        LEFT OUTER JOIN StatusNames as STMO WITH(NOLOCK) ON STMO.StatusID=MO.StatusID
        LEFT OUTER JOIN Addresses as ADSCUS WITH(NOLOCK) ON MO.CustomerID=ADSCUS.AddressID
        LEFT OUTER JOIN RawAllocations as RA WITH(NOLOCK) ON MO.ManufactureID=RA.ManufactureID
        LEFT OUTER JOIN RawMaterials as RM WITH(NOLOCK) ON RM.RawMaterialID=RA.RawMaterialID
        LEFT OUTER JOIN Colors as COLOR WITH(NOLOCK) ON COLOR.ColorID=RM.ColorID
        LEFT OUTER JOIN ComponentLibrary as COMPL WITH(NOLOCK) ON COMPL.ComponentID=RM.ComponentID 
        LEFT OUTER JOIN ComponentCategories as COMPC WITH(NOLOCK) ON COMPC.ComponentCategoryID=COMPL.ComponentCategoryID 
        LEFT OUTER JOIN ComponentSubcategories as COMPS WITH(NOLOCK) ON COMPS.SubCategoryID=COMPL.SubCategoryID
        LEFT OUTER JOIN Addresses as ADCVendor WITH(NOLOCK) ON ADCVendor.AddressID = COMPL.VendorID
        LEFT OUTER JOIN Warehouses as GodWarehouse WITH(NOLOCK) ON GodWarehouse.WarehouseID=MO.WarehouseID
        LEFT OUTER JOIN Warehouses as STOCK WITH(NOLOCK) ON STOCK.WarehouseID=MO.StockWarehouseID
        LEFT OUTER JOIN Warehouses as TRIMS WITH(NOLOCK) ON TRIMS.WarehouseID=MO.TrimWarehouseID
        LEFT OUTER JOIN EnumValues as DW WITH(NOLOCK) ON MO.MfgOrderTypeID=DW.EnumValueID
        LEFT OUTER JOIN BodyParts as BP WITH(NOLOCK) ON BP.BodyPartID = RA.BodyPartID 
    WHERE 
        MO.MfgOrderTypeID <> 820
        AND COMPC.CategoryName in ('Fabric','Trim')
        and MO.StatusID not in (20,90,95) -- Forecast 20 ,Complete 90 , Void 95
        AND RA.QuantityRequired > 0
    """

def Material_Coverage_Core():
    # =========================
    #  SQL Query - 
    # =========================
    print(str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + ' - Download Data for DB PolyPM' )
    inventory = Generate_query_result(SQL_Inventory)
    pos = helper_POs(Generate_query_result(SQL_Purchase_Orders)) 
    pos = pos.loc[pos['Status'] != 'Complete']
    mos = Generate_query_result(SQL_ManufactureOrders)

    # =========================
    #  MATERIAL COVERAGE - CORE
    # =========================
    print(str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))   + ' - Run Analysis Data (Core Material Coverage)')
    # Preparacion
    inventory = inventory.sort_values(by=["RawMaterialID"], ascending=[True])
    mos['Delivery_Balance'] = mos['Delivery_Balance'].astype(float)
    mos['StatusPN'] = mos['Status']
    pos = pos.sort_values(by=["ETA","RawMaterialID"], ascending=[True,True])
    mos = mos.sort_values(by=["PartNumber","PRD","SerialNumber"], ascending=[True,True,True])

    # Diccionario para rastrear stock por WarehouseID
    stock_warehouse = {row.WarehouseID: row.OnHand for _, row in inventory.iterrows()}

    # Filtro de órdenes pendientes
    pending_orders = mos[(mos['Status'].str.contains('Pending')) & (mos['Pending'] > 0)].copy()

    # Crear un diccionario de POs por RawMaterialID
    po_dict = {}

    # Iterar sobre todas las POs y agruparlas por RawMaterialID
    for po_idx, po_row in pos.iterrows():
        raw_material = po_row['RawMaterialID']
        if raw_material not in po_dict:
            po_dict[raw_material] = []
        po_dict[raw_material].append(po_row)

    # Ahora iterar sobre las MOs pendientes
    for idx, order in pending_orders.iterrows():
        raw_material = order['RawMaterialID']
        stock_warehouse = order['StockWarehouseID']
        
        # =========================
        #  CONSUMO EN MISMO WH
        # =========================
        stock = inventory[
            (inventory['RawMaterialID'] == raw_material) &
            (inventory['WarehouseID'] == stock_warehouse)
        ]

        for stock_idx, stock_row in stock.iterrows():
            pending = mos.at[idx, 'Pending']
            if pending <= 0:
                break

            on_hand = stock_row['OnHand']
            inspect = stock_row['Inspect']
            
            # ---- ON HAND ----
            if on_hand > 0:
                delivery = min(on_hand, pending)

                inventory.at[stock_idx, 'OnHand'] -= delivery
                mos.at[idx, 'Pending'] -= delivery
                mos.at[idx, 'Log'] += f' 1.0_(OnHand: {delivery}) --> '

                pending -= delivery

            if pending <= 0:
                mos.at[idx, 'Status'] = 'Cover'
                mos.at[idx, 'StatusPN'] = 'On Hand'
                break

            # ---- INSPECT ----
            if inspect > 0 and pending > 0:
                
                delivery = min(inspect, pending)

                inventory.at[stock_idx, 'Inspect'] -= delivery
                mos.at[idx, 'Pending'] -= delivery
                mos.at[idx, 'Log'] += f' 1.0_(Inspect: {delivery}) --> '
                mos.at[idx, 'Inspect'] = 'Inspect'

            mos.at[idx, 'Status'] = (
                'Cover'
                if mos.at[idx, 'Pending'] == 0
                else 'Pending - Less than pieces'
            )

            mos.at[idx, 'StatusPN'] = (
                'On Hand'
                if mos.at[idx, 'Pending'] == 0
                else 'Material Needed'
            )

        # =========================
        #  TRANSFERENCIAS
        # =========================
        if mos.at[idx, 'Pending'] > 0:
            other_warehouses = inventory[
                (inventory['RawMaterialID'] == raw_material) &
                (inventory['WarehouseID'] != stock_warehouse)
            ].sort_values(by="OnHand", ascending=False)

            for stock_idx, stock_row in other_warehouses.iterrows():
                pending = mos.at[idx, 'Pending']
                if pending <= 0:
                    break

                on_hand = stock_row['OnHand']
                inspect = stock_row['Inspect']

                # ---- ON HAND TRANSFER ----
                if on_hand > 0:
                    delivery = min(on_hand, pending)

                    inventory.at[stock_idx, 'OnHand'] -= delivery
                    mos.at[idx, 'Pending'] -= delivery
                    mos.at[idx, 'Log'] += (
                        f' 2.1_(WH: {stock_row["Warehouse"]}, Delivery: {delivery}) --> '
                    )

                    mos.at[idx, 'Delivery_Balance'] = float(delivery)
                    mos.at[idx, 'WH'] = stock_row['Warehouse']

                    pending -= delivery

                if pending <= 0:
                    mos.at[idx, 'Status'] = 'Cover (Transfer)'
                    mos.at[idx, 'StatusPN'] = 'On Hand'
                    break

                # ---- INSPECT TRANSFER ----
                if inspect > 0 and pending > 0:
                    delivery = min(inspect, pending)

                    inventory.at[stock_idx, 'Inspect'] -= delivery
                    mos.at[idx, 'Pending'] -= delivery
                    mos.at[idx, 'Log'] += (
                        f' 2.1_(WH Inspect: {stock_row["Warehouse"]}, Delivery: {delivery}) --> '
                    )
                    mos.at[idx, 'Inspect'] = 'Inspect'

                    mos.at[idx, 'Delivery_Balance'] = delivery
                    mos.at[idx, 'WH'] = stock_row['Warehouse']

                if mos.at[idx, 'Pending'] == 0:
                    mos.at[idx, 'Status'] = 'Cover (Transfer)'
                    mos.at[idx, 'StatusPN'] = 'On Hand'

        # =========================
        #  PURCHASE ORDER - LOGICA
        # =========================
        if mos.at[idx, 'Pending'] > 0:
            if raw_material in po_dict:
                for po_row in po_dict[raw_material]:
                    pending = mos.at[idx, 'Pending']
                    delivery = min(po_row['Delivery'], pending)

                    mos.at[idx, 'Pending'] -= delivery
                    po_row['Delivery'] -= delivery
                    if delivery > 0:
                        mos.at[idx, 'Log'] += f' 3.0_(PO: {po_row["PO"]}, ETA: {po_row["ETA"]}, Delivery: {delivery}) --> '
                        mos.at[idx, 'Delivery_Balance'] = delivery

                    mos.at[idx,'UnitCost'] = po_row[ 'UnitPrice']
                    mos.at[idx,'ETA'] =  po_row['ETA'] 
                    mos.at[idx,'PO'] = po_row[ 'PO'] 
                    mos.at[idx,'Vendor'] = po_row['Vendor'] 
                    mos.at[idx,'PreferentVendor'] = po_row[ 'Vendor']
                    mos.at[idx,'PurchasAgent']= po_row[ 'Agent']
                    mos.at[idx,'StockWarehousePO']=po_row[ 'Warehouse']
                    

                    if mos.at[idx, 'Pending'] == 0:
                        if mos.at[idx, 'PRD'] < po_row['ETA']:
                            mos.at[idx, 'Status'] = 'Out of ETA'
                            mos.at[idx, 'StatusPN'] = 'Late'
                        if mos.at[idx, 'PRD'] >= po_row['ETA']:
                            if po_row['ETA'] >= datetime.datetime.today():
                                mos.at[idx, 'Status'] = 'Cover with PO'
                            else:
                                mos.at[idx, 'Status'] = 'Cover with PO not received'
                            mos.at[idx, 'StatusPN'] = 'On Track'
                        if order['Warehouse'] != po_row['Warehouse']: ## La PO es de otra bodega
                            mos.at[idx, 'Status'] += ' TW'
                        break  # No necesita más POs
        
        # =========================
        #  REVISION DE PENDIENTES
        # =========================
        if mos.at[idx, 'Pending'] > 0 and mos.at[idx, 'Pending'] == mos.at[idx, 'Pending_static']:
            mos.at[idx, 'Status'] = 'Pending'
            mos.at[idx, 'StatusPN'] = 'Material Needed'
        if mos.at[idx, 'Pending'] > 0 and mos.at[idx, 'Pending'] != mos.at[idx, 'Pending_static']:
            mos.at[idx, 'Status'] = 'Pending - Less than pieces'
            mos.at[idx, 'StatusPN'] = 'Material Needed'
    
    # =========================
    #  PIVOT EXTRAS - 
    # =========================
    print(str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))  + ' - Pivots' )
    PV_MaterialNeeded,PV_MaterialNeeded_PN,PV_Aging,PV_Transfer,PV_PO_Risk,PV_PO_Category,pivot_graph_table = helper_Pivots(mos)
    # =========================
    #  CREANDO EXCEL
    # =========================
    print(str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))  + ' - Create XLSX Result' )
    import uuid
    myuuid = uuid.uuid4()
    writer = pd.ExcelWriter('MaterialCoverage_'+ str(myuuid) +'.xlsx', engine='xlsxwriter')
    mos[headers].to_excel(writer, sheet_name='Detail', index=False,freeze_panes=[1,0],float_format="%.2f")

    PV_MaterialNeeded.to_excel(writer, sheet_name='MaterialNeeded', index=True,freeze_panes=[1,0],float_format="%.2f")
    PV_MaterialNeeded_PN.to_excel(writer, sheet_name='MaterialNeeded-PN', index=True,freeze_panes=[1,0],float_format="%.2f")
    PV_Aging.to_excel(writer, sheet_name='MaterialNeded_Week_Future', index=True,freeze_panes=[1,0],float_format="%.2f")
    pivot_graph_table.to_excel(writer, sheet_name='Tendencia', index=True,freeze_panes=[1,0],float_format="%.2f")

    writer.close()
    print(str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))  + ' - End' )


Material_Coverage_Core()