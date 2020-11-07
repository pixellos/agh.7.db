import * as mongodb from 'mongodb';

const url = 'mongodb://localhost:27017';
const dbName = 'MateuszPopielarz1';
declare function emit(k, v);

// Use connect method to connect to the server
mongodb.connect(url, async function (err, client) {
  console.error(err);
  console.log("Connected successfully to server");
  const db = client.db(dbName);

  //await excerciseA(db);
  //await excerciseB(db);
  //await excerciseC(db);
  //await excerciseD(db);
  await excercise7(db);

  client.close();
});

async function excerciseA(db: mongodb.Db) {
  const count = await db.collection('yelp_academic_dataset_review').find({ stars: 5 }).count();
  console.log(`Zadanie A: Count: ${count}`);
}

async function excerciseB(db: mongodb.Db) {

  const data = await db.collection('yelp_academic_dataset_business').aggregate(
    [
      { $match: { categories: { $in: ["Restaurants"] } } },
      { $group: { "_id": "$city", "count": { $sum: 1 } } },
      { $sort: { "count": -1 } }
    ]
  ).toArray();

  console.log("Zadanie B", data);
}

async function excerciseC(db: mongodb.Db) {
  const data = await db.collection('yelp_academic_dataset_business')
    .aggregate(
      [
        {
          $match: {
            $and: [
              { categories: { $in: ["Hotels"] } },
              { "attributes.Wi-Fi": "free" },
              { stars: { $gt: 4.5 } }
            ]
          }
        },
        { $group: { "_id": "$state", "count": { $sum: 1 } } },
        { $sort: { "count": -1 } }
      ]
    ).toArray();

  console.log("Zadanie C", data);
}

async function excerciseD(db: mongodb.Db) {

  const data = await db.collection('yelp_academic_dataset_review').mapReduce(
    function () {
      if (this.votes.funny > 0)
      if (this.votes.cool > 0)
      if (this.votes.useful > 0)
        emit('all', 1);
    },
    function (key, values: number[]) {
      return values.reduce((x, acc) => x + acc);

    }, { out: 'reviews' as {} } as mongodb.MapReduceOptions);

  const result = await db.collection('reviews').find({}).toArray();
  console.log("Zadanie D", result);
}


async function excercise7(db: mongodb.Db) {
  const data = await db.collection('yelp_academic_dataset_review').aggregate(
    [
      { $match: { stars: { $gt: 4.5 } } },
      { $group: { "_id": "$user_id", "count": { $sum: 1 } } },
      { $sort: { "count": -1 } },
    ]
  ).toArray();
  const first = data[0];

  const userId = await db.collection('yelp_academic_dataset_user').findOne({ user_id: first['_id'] })
  console.log("Zadanie 7: ", userId['name']);
}
