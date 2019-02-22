package fr.insa.distml

import scopt.OptionParser
import play.api.libs.json._
import scala.reflect.io.Path
import fr.insa.distml.experiment._
import fr.insa.distml.experiment.ExperimentConfigReads._


object Main {

  def main(args: Array[String]): Unit = {

    // Define arguments parser
    val parser = new OptionParser[Arguments]("DistML") {
      opt[String]("config")
        .required()
        .valueName("<file|json>")
        .action((value, parameters) => parameters.copy(config = value))
        .text("The configuration in json or the path to the configuration file")
    }

    // Parsing arguments
    parser.parse(args, Arguments("")) match {
      case None             => Unit
      case Some(parameters) =>
        // Read experiment configuration from a specified file or directly from the argument
        val json = Path(parameters.config)
          .ifFile(file => Json.parse(file.inputStream()))
          .getOrElse(Json.parse(parameters.config))

        // Validate Json and start experiment
        json.validate[ExperimentConfig] match {
          case JsSuccess(config, _) => Experiment.from(config).perform()
          case JsError(errors)      => throw new IllegalArgumentException(errors.toString())
        }
    }
  }

  case class Arguments(config: String)
}
