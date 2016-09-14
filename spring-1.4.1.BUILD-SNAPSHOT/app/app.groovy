@RestController
class MyBootApp {
    @RequestMapping("/")
    def top() {
        "Hello Spring Boot!!"
    }
}
