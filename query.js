const PreparedStatement = require('pg-promise').PreparedStatement;
module.exports = {
    version1: new PreparedStatement({ name: 'version1', text: `` })
}