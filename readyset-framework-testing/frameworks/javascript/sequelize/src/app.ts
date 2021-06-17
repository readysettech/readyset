import {
    Sequelize,
    Model,
    Optional,
    DataTypes
} from "sequelize"

const db = require('./models')

async function main() {
    console.log(db.Model)

    const c1 = db["Commitment"].build({
        user: "prismaphonic",
        issue: "https://github.com/readysettech/readyset/issues/1",
        committedOn: new Date(),
        finishedOn: new Date()
    });
    await c1.save();
    console.log("Commitment Saved");

    await db["Commitment"].findAll().then( c => {
            console.log(c)
    })
}

main()
