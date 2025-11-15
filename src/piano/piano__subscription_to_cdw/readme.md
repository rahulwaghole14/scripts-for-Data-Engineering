I think that we're using email in the first instance. Then the user is given UID.
I don't think UID will match any records on our end. Email will be the unique identifier.

Each masthead has its own AID (app id). Each masthead has its own user repository. Each user will have their unique UID (user id).

When a user subscribes to eg The Post,
that's where their subscription details will be stored ie within that app.
But, I believe, that when they access another masthead site with their All Access subscription,
for which they need to log in. During the login process (as part of JWT token exchange)
a user will be created on each masthead instance as well.

https://hexanz.atlassian.net/wiki/spaces/SRIJ/pages/2695364614/Matrix+Piano+Entitlement+Mapping
