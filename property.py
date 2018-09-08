from sqlalchemy import Column, ForeignKey, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import exc
import json
from sqlalchemy import and_
from sqlalchemy import or_

from flask import Flask
from flask import request
from collections import OrderedDict


app = Flask(__name__)


Base = declarative_base()



#A class for each insurance/risk type
class Insurance(Base):
    __tablename__ = 'insurance'
    id = Column(Integer, primary_key=True)
    type = Column(String(256), nullable=False) #type of insurance/risk

class InsuranceAttribute(Base):
    __tablename__ = 'iattributes'
    id = Column(Integer, primary_key=True)
    insuranceID = Column(Integer, ForeignKey('insurance.id'))
    name = Column(String(256), nullable=False)     #name of the attribute/field
    dataType = Column(String(256), nullable=False)
    mandatory = Column(String(3), nullable=False)

class Insured(Base):
    __tablename__ = 'insured'
    id = Column(Integer, primary_key=True)
    insuranceID = Column(Integer, ForeignKey('insurance.id'))

class insuredData(Base):
    __tablename__ = 'insured_data'
    id = Column(Integer, primary_key=True)
    insuredID = Column(Integer, ForeignKey('insured.id'))
    name = Column(String(256))     #name of the attribute/field
    value = Column(String(4096))   #attribute/field value, opaque

DataTypes = {"int": 1,
             "enum" : 2,
             "string":3,
}

 # Create the engine that create in Memory DB

engine = create_engine('sqlite:///propertyInsurance.db')

# create a new insurance type
def createType(jsonObj):
    """
    Create a new Insurance type
    data: {"type":"health"}
    """
    Base.metadata.bind = engine
    DBSession = sessionmaker(bind=engine)
    session = DBSession()

    # check for "type", "optional" keyword exist in jsonObj
    if "type" not in jsonObj.keys():
        return json.dumps({"status":"error", "message":"Invalid request"}), 400

    # check for length of type value greater than 255
    if len(jsonObj["type"]) > 255:
        return json.dumps({"status":"error", "message":"Invalid request"}), 400

    # check for "insurance type" exists
    try:
        insurance = session.query(Insurance).filter(Insurance.type == jsonObj["type"]).all()
    except exc.SQLAlchemyError as e:
        return json.dumps({"status":"error", "message":"DB error"})

    if len(insurance):
        return json.dumps({"status":"error", "message":"Insurance type already exists"}), 409


    newInsurance = Insurance(type=jsonObj["type"])

    try:
        session.add(newInsurance)
        session.commit()
    except exc as e:
        sesion.rollback()
        return json.dumps({"status":"error", "message":"DB error"})
    finally:
        session.close()

    #return Response(response=json.dumps({"status":"success"}), status=201,mimetype='application/json')
    return json.dumps({"status":"success"}), 201, {'mimetype':'application/json'}



# get all insurance types
def getTypes():
    """
    Getting all Insurance Types    
    """
    Base.metadata.bind = engine
    DBSession = sessionmaker(bind=engine)
    session = DBSession()

    
    try:
        insurance_data = session.query(Insurance).all()
    except exc.SQLAlchemyError as e:
        return json.dumps({"status":"error", "message":"DB error"})


    insurance_types = []
    for ins_type in insurance_data:
        insurance_type = OrderedDict([("id", ins_type.id), ("type", ins_type.type)])
        insurance_types.append(insurance_type)

    if not len(insurance_types):
        return json.dumps({"status":"success", "insurances":[{}]}), 200

    return json.dumps({"status":"success", "insurances":insurance_types}), 200, {'Content-Type':'application/json'}



# add an attribute to an insurance type
def addAttribute(insuranceType, jsonObj):
    """
    Create Attributes to new Insurance Type
    Data: {"name":"fullName", "dataType":"String", "mandatory":"Yes"}    
    """
    Base.metadata.bind = engine
    DBSession = sessionmaker(bind=engine)
    session = DBSession()

    try:
        insurance = session.query(Insurance).filter(Insurance.type == insuranceType).one()
    except exc.SQLAlchemyError as e:
        return json.dumps({"status":"error", "message":"Insurance Type not found"}), 404

    attribute = InsuranceAttribute()

    # check if 'name', 'dataType' and 'mandatory' exists in the jsonObj before access
    if "name" not in jsonObj.keys() or "dataType" not in jsonObj.keys() or\
            "mandatory" not in jsonObj.keys():
        return json.dumps({"status":"error", "message":"Invalid request"}), 400

    # check if mandatory value other than 'yes' or 'no'
    if jsonObj["mandatory"].upper() != "YES" and jsonObj["mandatory"].upper() != "NO":
        return json.dumps({"status":"error","message":"Invalid request"}), 400

    # check for attribute already existed
    try:
        insurance_attr_data = session.query(InsuranceAttribute).filter(InsuranceAttribute.insuranceID == insurance.id).all()
    except exc.SQLAlchemyError as e:
        return json.dumps({"status":"error", "message":"DB error"})

    if insurance_attr_data:
        for attr in insurance_attr_data:
            if attr.name == jsonObj["name"]:
                return json.dumps({"status":"error", "message":"Attribute already exists"}), 409


    # check for attribute name lenght exceeds 255 and return proper error
    attribute.name = jsonObj["name"]
    if len(attribute.name) > 255:
        return json.dumps({"status":"error", "message":"Invalid request"}), 400

    # check if the given data type matches with one of the dataTypes, like 'int', string'
    if jsonObj["dataType"] not in DataTypes.keys():
        return json.dumps({"status":"failed", "message":"Invalid request"}), 400

    # check for optional value should be yes or no
    if jsonObj["mandatory"].upper() != "YES" and jsonObj["mandatory"].upper() != "NO":
        return json.dumps({"status":"error", "message":"Invalid request"}), 400

    attribute.dataType = jsonObj["dataType"]
    attribute.insuranceID = insurance.id
    attribute.mandatory = jsonObj["mandatory"]

    try:
        session.add(attribute)
        session.commit()
    except exc.SQLAlchemyError as e:
        session.rollback()
        return json.dumps({"status":"error", "message":"DB error"})
    finally:
        session.close()

    return json.dumps({"status":"success"}), 201


# get all attributes of an insurance type
def getAttributes(insuranceType):
    """
    Get all Attributes of Insurance Type
    """
    Base.metadata.bind = engine
    DBSession = sessionmaker(bind=engine)
    session = DBSession()

    # check for insurance type
    try:
        insurance = session.query(Insurance).filter(Insurance.type == insuranceType).one()
    except exc.SQLAlchemyError as e:
        return json.dumps({"status":"error", "message":"Insurance Type not found"}), 404

    # getting all insurance attributes data
    try:
        insurance_attributes = session.query(InsuranceAttribute) \
                            .filter(InsuranceAttribute.insuranceID == insurance.id).all()
    except exc.SQLAlchemyError as e:
        return json.dumps({"status":"error", "message": "DB error"})

    attributes = []
    for insurance_attribute in insurance_attributes:
        attribute = OrderedDict([("name", insurance_attribute.name), \
                    ("dataType", insurance_attribute.dataType), \
                    ("mandatory", insurance_attribute.mandatory)])
        attribute_name = {insurance_attribute.name:attribute}
        attributes.append(attribute_name)

    if not len(attributes):
        return json.dumps({"status":"success", "attributes":[{}]}), 200

    return json.dumps({"status":"success", "attributes":attributes}), 200



# add an insured to an the data base
def addInsured(insuranceType, jsonObj):
    """
    Add Insured for Insurance Type, 
    data: {"attributes":[{"attributeName":"fullName", "attributeValue":"value of attribute"}]}
    """
    Base.metadata.bind = engine
    DBSession = sessionmaker(bind=engine)
    session = DBSession()

    # check for insurance type
    try:
        insurance = session.query(Insurance).filter(Insurance.type == insuranceType).one()
    except exc.SQLAlchemyError as e:
        return json.dumps({"status":"error", "message":"Insurance Type not found"}), 404

    # Check if 'attributes' is in jsonObj
    if 'attributes' not in jsonObj:
        return json.dumps({"status":"error", "message":"Invalid request"}), 400

    # check for all attribute names exist for insurance type
    try:
        insurance_attr_data = session.query(InsuranceAttribute).filter(InsuranceAttribute.insuranceID == insurance.id).all()
    except exc.SQLAlchemyError as e:
        return json.dumps({"status":"error", "message":"DB error"})

    insurance_attributes = []
    insurance_mandatory_attributes = []
    for insurance_attr in insurance_attr_data:
        insurance_attributes.append(insurance_attr.name)
        if insurance_attr.mandatory.upper() == "YES":
            insurance_mandatory_attributes.append(insurance_attr.name)

    attributes = jsonObj["attributes"]
    # check for all attributes exist in insurance type attributes
    for attr in attributes:
        if attr["attributeName"] not in insurance_attributes:
            return json.dumps({"status":"error", "message": attr["attributeName"] + " attribute not exist for Inurance type"}), 404


    # check for all mandatory attributes exists in the attributes
    request_attr_names = []
    for attr in attributes:
        request_attr_names.append(attr["attributeName"])

    for attr in insurance_mandatory_attributes:
        if attr not in request_attr_names:
            return json.dumps({"status":"error","message":"Invalid request"}), 400

    #add insured entry
    ins = Insured()
    ins.insuranceID = insurance.id;

    try:
        session.add(ins)
        session.commit()
    except exc.SQLAlchemyError as e:
        session.rollback()
        return json.dumps({"status":"error", "message":"DB error"})



    for attr in attributes:
        try:
            iattr = session.query(InsuranceAttribute).filter(and_( \
                    InsuranceAttribute.insuranceID == insurance.id, \
                    InsuranceAttribute.name == attr["attributeName"])).one()
        except exc.SQLAlchemyError as e:
            return json.dumps({"status":"error", "message": attr["attributeName"] + " " +"Attribute not found"}), 404

        if iattr.dataType=="int":
            # check for valid integer value, throw error if not valid
            try:
                value = int(attr["attributeValue"])
            except ValueError:
                return json.dumps({"status":"error", "message":"Invalid request"})
        elif iattr.dataType=="enum":
            value = attr["attributeValue"]
        elif iattr.dataType=="string":
            value = attr["attributeValue"]
        else:
            return  json.dumps({"status":"failed", "message":"Unknown"})

        newAttr = insuredData()
        newAttr.insuredID = ins.id
        newAttr.name = attr["attributeName"]
        newAttr.value = value

        try:
            session.add(newAttr)
            session.commit()
        except exc.SQLAlchemyError as e:
            return json.dumps({"status":"error", "message":"DB error"})


    return json.dumps({"status":"success", "insuranceID":ins.id}), 201



# getone insured detials
def getOneInsured(insuranceType, insuredID):
    """
    Get One Insured details to an Insurance Type
    """
    
    Base.metadata.bind = engine
    DBSession = sessionmaker(bind=engine)
    session = DBSession()

    try:
        insurance = session.query(Insurance).filter(Insurance.type == insuranceType).one()
    except exc.SQLAlchemyError as e:
        return json.dumps({"status":"error", "message":"Insurance Type not found"}), 404

    try:
        insured = session.query(Insured).filter(Insured.id == insuredID).one()
    except exc.SQLAlchemyError as e:
        return json.dumps({"status":"error", "message":"Insured not found"}), 404

    if insured.insuranceID != insurance.id:
        return json.dumps({"status":"error", "message":"Insured not found"}), 404


    try:
        iData = session.query(insuredData).filter(insuredData.insuredID == insuredID).all()
    except exc.SQLAlchemyError as e:
        return json.dumps({"status":"error", "message":"Insured not found"}), 404


    list = []
    # TODO Check for maximum number of items can be returned based on the configured value.
    for i in iData:
        x = OrderedDict([("name", i.name), ("value", i.value)])
        attr = {i.name:x}
        list.append(attr)

    return json.dumps(OrderedDict([("status", "success"), ("insurediD", insuredID), \
                    ("insuranceType", insuranceType), ("attributes", list)])), 200



def getAllInsured(insuranceType):
    """
    Get All Insured for an insurance type
    """
    
    Base.metadata.bind = engine
    DBSession = sessionmaker(bind=engine)
    session = DBSession()

    # check for insurance type exist
    try:
        insurance = session.query(Insurance).filter(Insurance.type == insuranceType).one()
    except exc.SQLAlchemyError as e:
        return json.dumps({"status":"error", "message":"Insurance Type not found"}), 404
    
    # getting all insured for an insurance type
    try:
        iData = session.query(Insured).filter(insurance.id == Insured.insuranceID).all()
    except exc.SQLAlchemyError as e:
        return json.dumps({"status":"error", "message":"DB error"})

    ID = []
    if not len(iData):
        return json.dumps(OrderedDict([("status", "success"),("type", insurance.type),("insuredID",ID)])), 200


    for i in iData:
        ID.append(i.id)

    return json.dumps(OrderedDict([("status", "success"),("type", insurance.type),("insuredID",ID)])), 200


def updateInsured(insuranceType, insuredID, jsonObj):
    """
    Update attribute value of insured to an insurance type
    data: {"attributes":{"attributeName":"fullName", "attributeValue":"fullname1_modified"}}
    """
    Base.metadata.bind = engine
    DBSession = sessionmaker()
    session = DBSession()

    # check for insurance type exists
    try:
        insurance = session.query(Insurance).filter(Insurance.type == insuranceType).one()
    except exc.SQLAlchemyError as e:
        return json.dumps({"status":"failure", "message":"Insurance Type not found"}), 404

    # check for "attributes" exist in jsonObj
    if "attributes" not in jsonObj:
        return json.dumps({"status":"failure", "message":"Invalid request"}), 400
    attribute = jsonObj["attributes"]

    # check for "attributeName", "attributeValue" in attributes
    if "attributeName" not in attribute.keys() or "attributeValue" not in attribute.keys():
        return json.dumps({"status":"error", "message":"Invalid request"}), 400


    # check for insuredID exists
    try:
        insured = session.query(Insured).filter(Insured.id == insuredID).one()
    except exc.SQLAlchemyError as e:
        return json.dumps({"status":"failure", "message":"Insured not found"}), 404

    if insured.insuranceID != insurance.id:
        return json.dumps({"status":"failure", "message":"Insured not found"}), 404

    # getting insurance attributes
    try:
        iattr = session.query(InsuranceAttribute).filter(InsuranceAttribute.insuranceID == insurance.id).all()
    except exc.SQLAlchemyError as e:
        return json.dumps({"status":"error", "message": "DB error"})

    insurance_attributes = []
    for attr in iattr:
        insurance_attributes.append(attr.name)

    # check for attributeName exist for insurance type
    if attribute["attributeName"] not in insurance_attributes:
        return json.dumps({"status":"error","message":attribute["attributeName"] + " not found for insurance type"}), 404

    # getting all attributes added to insured
    try:
        iData = session.query(insuredData).filter(insuredData.insuredID == insured.id).all()
    except exc.SQLAlchemyError as e:
        return json.dumps({"status":"error", "message":"DB error"})

    insured_attributes = []
    for i in iData:
        insured_attributes.append(i.name)


    # getting insurance attribute
    try:
        iattr = session.query(InsuranceAttribute).filter(and_(InsuranceAttribute.insuranceID == insurance.id,InsuranceAttribute.name == attribute["attributeName"])).one()
    except exc.SQLAlchemyError as e:
        return json.dumps({"status":"error","message":"DB error"})


    # check for valid datatype of attribute value
    if iattr.dataType == "int":
        try:
            value = int(attribute["attributeValue"])
        except ValueError:
            return json.dumps({"status":"error", "message":"Invalid data Type"}), 400
    elif iattr.dataType == "enum":
        value = attribute["attributeValue"]
    elif iattr.dataType == "string":
        value = attribute["attributeValue"]
    else:
        return json.dumps({"status":"error", "message":"data type not found"}), 404

    # if attribute found in insured attributes
    if attribute["attributeName"] in insured_attributes:
        # getting insured data
        try:
            iData = session.query(insuredData).filter(and_(insuredData.insuredID == insured.id, insuredData.name == attribute["attributeName"])).one()
        except exc.SQLAlchemyError as e:
            return json.dumps({"status":"error", "message":"Attribute not found"}), 404

        try:
            iData.value = value
            session.add(iData)
            session.commit()
        except exc.SQLAlchemyError as e:
            session.rollback()
            return json.dumps({"status":"error", "message":"DB error"})
    else:
        newAttr = insuredData()
        newAttr.insuredID = insured.id
        newAttr.name = attribute["attributeName"]
        newAttr.value = value
        try:
            session.add(newAttr)
            session.commit()
        except exc.SQLAlchemyError as e:
            session.rollback()
            return json.dumps({"status":"error","message":"DB error"})

    return json.dumps({"status":"success"}), 200



def deleteInsured(insuranceType, insuredID):
    """
    Delete Insured to an Insurance Type
    """
    
    Base.metadata.bind = engine
    DBSession = sessionmaker(bind=engine)
    session = DBSession()

    try:
        insurance = session.query(Insurance).filter(Insurance.type == insuranceType).one()
    except exc.SQLAlchemyError as e:
        return json.dumps({"status":"error", "message":"Insurance type not found"}), 404

    try:
        insured = session.query(Insured).filter(Insured.id == insuredID).one()
    except exc.SQLAlchemyError as e:
        return json.dumps({"status":"error", "message":"Insured not found"}), 404

    if insured.insuranceID != insurance.id:
        return json.dumps({"status":"failure", "message":"Insured not found for insurance type" + " " + insurance.type}), 404

    # Delete insurenceData and insuedID
    try:
        iDataDeletedRows = session.query(insuredData).filter(insuredData.insuredID == insuredID).delete()
        deleteInsuredID = session.query(Insured).filter(Insured.id == insuredID).delete()
        session.commit()
    except exc.SQLAlchemyError as e:
        session.rollback()
        return json.dumps({"status":"failure", "message":"DB Error"})
    finally:
        session.close()

    return json.dumps({"status":"success"}), 200



def deleteType(insuranceType):
    """
    Delete Insurance Type
    """
    Base.metadata.bind = engine
    DBSession = sessionmaker(bind=engine)
    session = DBSession()

    # check for insurance type
    try:
        insurance = session.query(Insurance).filter(Insurance.type == insuranceType).one()
    except exc.SQLAlchemyError as e:
        return json.dumps({"status":"error", "message":"Insurance type not found"}), 404

    # check for weather any insured values for insurance type
    try:
        iData = session.query(Insured).filter(Insured.insuranceID == insurance.id).all()
    except exc.SQLAlchemyError as e:
        return json.dumps({"status":"error", "message":"DB error"})

    if iData:
        return json.dumps({"status":"error", "message":"Insured depends on insurance type"})


    # delete insurance type
    try:
        deletedRows = session.query(Insurance).filter(Insurance.type == insuranceType).delete()
        aData = session.query(InsuranceAttribute).filter(InsuranceAttribute.insuranceID == insurance.id).delete()
        session.commit()
    except exc.SQLAlchemyError as e:
        session.rollback()
        return json.dumps({"status":"error", "message":"DB error"})

    return json.dumps({"status":"success"}), 200




def deleteAttribute(insuranceType, jsonObj):
    """
    Delete Attribute of Insurance Type
    """
    Base.metadata.bind = engine
    DBSession = sessionmaker(bind=engine)
    session = DBSession()

    # check for "name" exist in jsonObj
    if "name" not in jsonObj.keys():
        return json.dumps({"status":"error","message":"Invalid request"}), 400

    # check for insurance type exist
    try:
        insurance = session.query(Insurance).filter(Insurance.type == insuranceType).one()
    except exc.SQLAlchemyError as e:
        return json.dumps({"status":"error","message":"Insurance Type not found"}), 404

    # check for attribute exist for insurance type
    try:
        iattr = session.query(InsuranceAttribute).filter(and_(InsuranceAttribute.insuranceID == insurance.id, InsuranceAttribute.name == jsonObj["name"])).one()
    except exc.SQLAlchemyError as e:
        return json.dumps({"status":"error","message":jsonObj["name"] + " attribute not found"}), 404

    # getting insured for an insurance type
    try:
        insData = session.query(Insured).filter(Insured.insuranceID == insurance.id).all()
    except exc.SQLAlchemyError as e:
        return json.dumps({"status":"error","message":"DB error"})


    # check for attribute name in insured data for insurance type
    for ins in insData:
        try:
            iData = session.query(insuredData).filter(and_(insuredData.insuredID == ins.id, insuredData.name == jsonObj["name"])).all()
        except exc.SQLAlchemyError as e:
            return json.dumps({"status":"error", "message":"DB error"})

        if len(iData):
            return json.dumps({"status":"error","message":jsonObj["name"] + " attribute is busy for an insurance type" })


    # delete insurnce attrubute
    if not len(insData) or not len(iData):
        try:
            delRows = session.query(InsuranceAttribute).filter(and_(InsuranceAttribute.insuranceID == insurance.id, InsuranceAttribute.name == jsonObj["name"])).delete()
            session.commit()
        except exc.SQLAlchemyError as e:
            session.rollback()
            return json.dumps({"status":"error", "message":"DB error"})

    return json.dumps({"status":"success"}), 200






@app.route('/risk/<riskId>/deleteAttribute', methods=['POST'])
def deleteRiskAttribute(riskId):
    args = request.args
    jsonObj = json.loads(request.data)
    return deleteAttribute(riskId,jsonObj)


@app.route('/risk/getAll',methods=['GET'])
def getAllRisks():
    return getTypes()

@app.route('/risk/<riskId>/getAttributes',methods=['GET'])
def getRiskAttributes(riskId):
    return getAttributes(riskId)

@app.route('/risk/<riskId>/addAttribute', methods=['POST'])
def addRiskAttribute(riskId):
    jsonObj = json.loads(request.data)
    return addAttribute(riskId, jsonObj)

@app.route('/risk/<riskId>/addInsured', methods=['POST'])
def addRiskInsured(riskId):
    jsonObj = json.loads(request.data)
    return addInsured(riskId, jsonObj)

@app.route('/risk/<riskId>/get/<insuredID>', methods=['GET'])
def getRiskInsured(riskId, insuredID):
    return getOneInsured(riskId, insuredID)


@app.route('/risk/create', methods=['POST'])
def addRisk():
    #print 'Request to add risk', request.data
    jsonObj = json.loads(request.data)
    #print 'creating new risk:', jsonObj["type"]
    return createType(jsonObj)


@app.route('/risk/<riskId>/getAll')
def getAllRiskInsured(riskId):
    return getAllInsured(riskId)


@app.route('/risk/<riskId>/update/<insuredID>', methods=['PUT'])
def updateRiskInsured(riskId, insuredID):
    jsonObj = json.loads(request.data)
    return updateInsured(riskId, insuredID, jsonObj)


@app.route('/risk/delete/<riskId>', methods=['POST'])
def deleteRisk(riskId):
    return deleteType(riskId)



@app.route('/risk/<riskId>/delete/<insuredID>', methods=['POST'])
def deleteRiskInsured(riskId, insuredID):
    return deleteInsured(riskId, insuredID)





def main():


    # Create all tables
    Base.metadata.create_all(engine)

    app.run(debug=True)






if __name__ == "__main__":
    main()
