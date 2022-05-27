function addReadImagesRoleToUser(user, context, callback) {
    var request = require("request");

    var count = context.stats && context.stats.loginsCount ? context.stats.loginsCount : 0;
    if (count > 1) {
        return callback(null, user, context);
    }

    var headers = {
        'Authorization': 'Bearer ' + auth0.accessToken,
    };
    const data = {
        "roles": [
            "${TERRAFORM_ROLE_ID}"
        ]
    };

    request.post({
        url: `https://$${auth0.domain}/api/v2/users/$${user.user_id}/roles`,
        headers: headers,
        json: data
    }, (err, response, body) => {
        return callback(new Error("Can not update users with role"));
    });

    callback(null, user, context);
}