Users
Operations on users belonging to an organization

Get Users
Queries multiple user identities in the organization domain. Filtering is available.



QUERY PARAMETERS
filter	
string
Without a filter, all users in a user domain are returned. The filter parameter must be a properly formed SCIM filter using either the operator eq (equals) or the operator sw (starts with). The filter works for userName, displayName, name.givenName, and name.familyName attributes. For example, /Users?filter=name.familyName%20eq%20%%22Smith%22

startIndex	
integer
The 1-based index of the first result in the current set of list results. REQUIRED when partial results are returned due to pagination.

count	
integer
Non-negative integer. Specifies the desired number of query results per page, example: 10. A negative value shall be interpreted as "0". A value of "0" indicates that no resource results are to be returned except for total results.

HEADER PARAMETERS
Authorization
required
string
Access token prefixed with 'Bearer ', e.g. 'Bearer 123456abcdef'

Responses
200 The request has succeeded.
400 Invalid filter syntax
401 Client is not sufficiently authorized
403 Invalid token passed
502 Authentication or account gateway error occurred
504 Authentication or account gateway timeout occurred