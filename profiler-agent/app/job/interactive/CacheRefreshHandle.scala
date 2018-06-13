package job.interactive

import javax.inject.{Inject, Singleton}

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
@Singleton
class CacheRefreshHandle @Inject()(livyInteractiveRunner: LivyInteractiveRunner, cache: LivyQueryCache) {

  def clearCache(): Future[Unit] = {
    livyInteractiveRunner.refreshAllTables()
      .flatMap(_ =>
        cache.clearCache()
      )
  }
}
