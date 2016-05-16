# mpcs52040-project

### Continent 
- Manually tested but need some tweaks to be enabled --- since it is not needed for this phase(as suggested by the test cases). 
- It acts only as a DNS server, hosts the RPC stockLookUp, which will return the address, if exists, of the market that has the stock.

### Market
- Keeps a user table of user credentials as well as the stocks each user has.
- Provided methods for exchanges: 
  - sellStock: if stocks exists locally, does the operation. Or otherwise return the address of the Market that has it.
  - buyStock: similar to sellStock
  - register: create a user record, balance initialized to 10000
  - login: logs in a user and returns an Investor object that has updated balance and stock it holds
  
### User
- User interface
- Accepted commands:
  - register,username,password
  - sell,stockname,amount
  - buy,stockname,amount
  - summary
