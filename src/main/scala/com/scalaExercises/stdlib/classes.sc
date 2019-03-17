
def maybeItWillReturnSomething(flag: Boolean): Option[String] = {
  if (flag) Some("Found value") else None
}
val value1 = maybeItWillReturnSomething(true)
val value2 = maybeItWillReturnSomething(false)
value1.isEmpty
value2.isEmpty
value1 getOrElse "No value"
value2 getOrElse "No value"
value1 getOrElse {
  "default function"
}

value2 getOrElse {
  "default function"
}

val someValue: Option[Double] = Some(20.0)
val value = someValue match {
  case Some(v) => v
  case None => 0.0
}


val number: Option[Int] = Some(3)
val noNumber: Option[Int] = None
val result1 = number.map(_ * 1.5)
val result2 = noNumber.map(_ * 1.5)

val result11 = number.fold(0)(_ * 5)
val result22 = noNumber.fold(0)(_ * 3)

object Greeting {
  def english = "Hi"

  def espanol = "Hola"
}

val x = Greeting
val y = x
x eq y
val z = Greeting

x eq z

class Movie(val name: String, val year: Short)

object Movie {
  def academyAwardBestMoviesForYear(x: Short) = {
    //This is a match statement, more powerful than a Java switch statement!
    x match {
      case 1930 ⇒ Some(new Movie("All Quiet On the Western Front", 1930))
      case 1931 ⇒ Some(new Movie("Cimarron", 1931))
      case 1932 ⇒ Some(new Movie("Grand Hotel", 1932))
      case _ ⇒ None
    }
  }
}
Movie.academyAwardBestMoviesForYear(1932).get.name



class Person(val name: String, private val superheroName: String) //The superhero name is private!
object Person {
  def showMeInnerSecret(x: Person) = x.superheroName
}
val clark = new Person("Clark Kent", "Superman")
val peter = new Person("Peter Parker", "Spider-Man")

