resolvers += Resolver.url("bintray-sbt-plugins", url("https://dl.bintray.com/eed3si9n/sbt-plugins/"))(Resolver.ivyStylePatterns)


addSbtPlugin("com.eed3si9n"            % "sbt-assembly"    % "0.14.8")

addSbtPlugin("com.sksamuel.scapegoat" %% "sbt-scapegoat"   % "1.0.9")
addSbtPlugin("org.wartremover"         % "sbt-wartremover" % "2.4.1")
addSbtPlugin("com.geirsson"            % "sbt-scalafmt"    % "1.5.1")
