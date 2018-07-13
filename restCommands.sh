#get all risk types
curl http://localhost:5000/risk/getAll

#add a new risk type
curl -H "Content-Type: application/json" -X POST -d '{"type":"health"}' http://localhost:5000/risk/create

#add attribute/field to a risk type
curl -H "Content-Type: application/json" -X POST -d '{"name":"age", "dataType":"int", "mandatory":"yes"}' http://localhost:5000/risk/health/addAttribute

curl http://localhost:5000/risk/health/getAttributes

# add insured to an insurance type
curl -H "Content-Type: application/json" -X POST -d '{"attributes":[{"attributeName":"age", "attributeValue":"64"}, {"attributeName":"address", "attributeValue":"flatno4102, hyderabad"}]}' http://localhost:5000/risk/health/addInsured

# get one insured data
curl  http://localhost:5000/risk/health/get/1

# get all insured for insurance type
curl http://localhost:5000/risk/health/getAll

# update insured data 
curl -i -H "content-type:application/json" -X PUT -d '{"attributes":{"attributeName":"fullName", "attributeValue":"fullname1_modified"}}' http://localhost:5000/risk/health/update/1

# delete insured to an insurance type
curl -H "Content-Type: application/json" -X POST http://localhost:5000/risk/health/delete/1

# delete risk type
curl -H "Content-Type: application/json" -X POST http://localhost:5000/risk/delete/health 


# delete risk attribute
curl -H "Content-Type: application/json" -X POST -d '{"name":"voterid"}' http://localhost:5000/risk/health/deleteAttribute
