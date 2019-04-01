package fr.datamantra.jobinterview.model


case class Results(
                    topTenEmittersLastTenYears: List[String],
                    smallestEmittersByYear: Map[String, List[String]],
                    topFiveIncreaseFrom1980to2000: List[String]
                  )




