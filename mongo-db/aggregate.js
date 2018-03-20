db.restaurants.aggregate(
                          [
                            {
					                    $unwind:"$address"
                            },
                            {
                              $unwind:"$gradess"
                            },
                            { $match : { "grades.grade": "A" }
                            },
                            {
                              $group:
                              {
                                "_id": "$cuisine"
                                "MIN":
                                  {
                                    $min:{ }
                                  }
                              }
                            },
                            {
                              $project:{
						                          "_id":0,
						                          "IDENTIFICADOR":"$_id.ID",
						                          "NOMBRE COMPLETO":"$_id.NOM" ,
						                          "TOTAL CLIENTE": 1
					                    }
                            },
                            {
                              $out: "rest_aggregate"
                            }
                          ]
                      )
