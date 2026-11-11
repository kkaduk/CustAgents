Jak architektura Banking Situation Awareness może spełnić potrzeby klienta?

1. Ocena ogólna

Architektura opisana w dokumencie „Banking Situation Awareness” bardzo dobrze odpowiada na przedstawione potrzeby klienta. Co więcej, te wymagania pokazują, dlaczego połączenie Apache Pekko, Oracle Exadata, Oracle Property Graph, Kafka i Apache Flink może być wartościowsze niż samodzielne zastosowanie bazy grafowej.

Wszystkie pięć potrzeb łączy jedna idea:

Chcemy rozumieć sytuację klienta nie tylko na podstawie jego własnych danych, ale również na podstawie struktury relacji, zachowań i ryzyka występujących w jego otoczeniu.

To jest naturalne rozszerzenie koncepcji Situation Awareness przedstawionej w dokumencie.

Proponowany system już zakłada dwa poziomy inteligencji:

* Local Situation Intelligence (Apache Pekko) — rozumienie aktualnej sytuacji pojedynczego klienta.
* Global Situation Intelligence (Oracle Exadata) — analiza szerszych zależności, sieci powiązań i wzorców ryzyka.

Dodatkowo dokument przewiduje, że wyniki analiz globalnych mogą wracać do agentów jako zdarzenia, zmieniając ich ocenę sytuacyjną. ExaPekdoDesign.md

To istotne, ponieważ klient nie oczekuje wyłącznie możliwości wykonywania zapytań grafowych. Chce również wykorzystywać uzyskane informacje do poprawy modeli, monitorowania ryzyka i analizowania zmieniających się relacji.

Ważne rozróżnienie: dokument definiuje architekturę umożliwiającą realizację tych scenariuszy, ale nie zawiera jeszcze konkretnych algorytmów AML, identyfikacji beneficjentów rzeczywistych ani mechanizmu budowania cech grafowych dla modeli ML. Te elementy trzeba zaprojektować jako rozszerzenia istniejącej architektury.

⸻

2. Mapowanie potrzeb klienta na komponenty systemu

Potrzeba klienta	Proponowany mechanizm	Główna technologia	Rezultat
1. Charakterystyka otoczenia klienta	Wieloskokowa analiza grafu i agregowanie cech sąsiedztwa	Oracle Property Graph + Pekko	Nowe cechy do modeli ML
2. Powiązania z listami restrykcyjnymi	Wyszukiwanie ścieżek do podmiotów objętych restrykcjami	Oracle Property Graph + RiskSignalAgent	Sygnały ekspozycji na ryzyko
3. Identyfikacja beneficjentów rzeczywistych	Przechodzenie przez wielopoziomowe struktury własności i kontroli	Oracle Property Graph + RelationshipAgent	Analiza UBO i pośredniej ekspozycji
4. Wykrywanie obiegu pieniędzy	Analiza cykli i przepływów w grafie transakcyjnym	Oracle + Flink + Pekko	Wykrywanie podejrzanych schematów AML
5. Powiązania osób i firm	Heterogeniczny graf obejmujący osoby, firmy, rachunki i relacje	Oracle Property Graph + Pekko	Wielowymiarowy obraz relacji klienta

Kluczowe jest to, że Oracle powinien odpowiadać za analizę globalnej sieci, a Pekko za utrzymywanie i aktualizowanie znaczenia wyników tej analizy dla konkretnego klienta.

Takie rozdzielenie odpowiedzialności jest zgodne z podstawową decyzją architektoniczną dokumentu: Oracle nie powinien rekonstruować całej sytuacji klienta przy każdym żądaniu. ExaPekdoDesign.md

⸻

3. Potrzeba pierwsza: charakterystyka otoczenia klienta i poprawa modeli

Wymaganie

Chcemy coś więcej powiedzieć o kliencie na podstawie jego otoczenia — na przykład ilu klientów w kilku skokach ma codzienne wydatki, a ilu ich nie ma — i sprawdzić, czy takie informacje poprawiają modele.

Jak system to realizuje?

System może wykorzystywać Oracle Global Relationship Graph do wyznaczania cech opisujących otoczenie klienta.

Przykładowo:

                 Customer A
                     |
          +----------+----------+
          |                     |
      Customer B             Company X
          |                     |
      Customer C             Customer D
          |
      Customer E

Załóżmy, że chcemy sprawdzić zachowania klientów w odległości maksymalnie trzech skoków od klienta A.

Oracle analizuje graf i identyfikuje odpowiednie podmioty.

Następnie system może obliczyć:

Customer A
Network Features:
neighbors_1_hop: 2
neighbors_2_hops: 2
neighbors_3_hops: 1
customers_with_daily_expenses_3h: 3
customers_without_daily_expenses_3h: 1
daily_expenses_ratio_3h: 0.75

Wartości są przykładowe. Produkcyjnie należy zdefiniować, które typy relacji są dopuszczalne i jak traktować firmy występujące na ścieżkach.

Jak wykorzystać te informacje?

Proponuję wprowadzić nową kategorię danych:

Network Context Features

Mogłaby zawierać m.in.:

Cecha	Znaczenie
daily_expenses_ratio_2h	Odsetek klientów z regularnymi wydatkami w odległości dwóch skoków
daily_expenses_ratio_3h	Analogiczny wskaźnik dla trzech skoków
network_income_stability	Agregat stabilności dochodów w otoczeniu
network_risk_exposure	Udział podmiotów posiadających określone sygnały ryzyka
network_transaction_activity	Aktywność transakcyjna wybranego otoczenia
network_behavior_similarity	Podobieństwo zachowania klienta do otoczenia

Integracja z architekturą

Oracle Global Relationship Graph
             |
             v
     Network Feature
       Computation
             |
             v
    Network Feature Store
             |
             +------------------+
             |                  |
             v                  v
      ML Training         Kafka Event
                                |
                                v
                         CustomerAgent
                                |
                                v
                       CustomerSituation

Dokument przewiduje globalne wskaźniki sieciowe oraz przekazywanie wyników analizy do agentów w postaci zdarzeń. ExaPekdoDesign.md

Co trzeba dodać?

Warstwę obliczania i wersjonowania cech grafowych, pozwalającą wykorzystywać te same definicje podczas trenowania modeli i ich produkcyjnego użycia.

Warto również zadbać o poprawną walidację przyrostowej wartości cech. Nie wystarczy wykazać korelacji — trzeba sprawdzić, czy nowe cechy poprawiają model poza próbą, przy zachowaniu chronologii danych, bez wycieku informacji z przyszłości.

Korzyść biznesowa

Klient uzyskuje możliwość wzbogacenia istniejących modeli ryzyka i scoringu o kontekst sieciowy.

Co istotne, Pekko nie musi samodzielnie przeszukiwać wielomilionowego grafu.

Otrzymuje gotowe, istotne dla klienta informacje, które może włączyć do jego aktualnej sytuacji.

⸻

4. Potrzeba druga: wykrywanie powiązań z listami restrykcyjnymi

Wymaganie

Chcemy zobaczyć, czy klient albo firma jest w kilku skokach powiązana z kimś z listy restrykcyjnej.

To klasyczny scenariusz analizy grafowej związanej z sankcjami, KYC i AML.

Jak system to realizuje?

Oracle utrzymuje graf relacji pomiędzy osobami, firmami i innymi podmiotami.

Przykład:

Customer A
    |
    | owns
    v
Company B
    |
    | controlled_by
    v
Person C
    |
    | owns
    v
Company D
    |
    | sanctioned
    v
SANCTIONS LIST

System powinien umożliwiać znalezienie odpowiedzi na pytania:

* Czy klient jest bezpośrednio powiązany z podmiotem objętym sankcjami?
* Czy istnieje takie powiązanie w odległości dwóch, trzech lub więcej skoków?
* Jakie typy relacji tworzą tę ścieżkę?
* Czy relacja jest aktualna?
* Czy pośrednie powiązanie wynika z własności, kontroli czy przepływów finansowych?

Proponowany mechanizm

Oracle analizuje graf i identyfikuje ścieżkę do podmiotu objętego restrykcjami.

Wynik może przyjąć postać:

SanctionsExposureDetected
subjectId: CUSTOMER-123
sanctionedEntityId: ENTITY-987
distance: 3
relationshipPath:
    CUSTOMER-123
      -> COMPANY-456
      -> PERSON-789
      -> ENTITY-987
relationshipTypes:
    OWNERSHIP
    CONTROL
    OWNERSHIP
detectedAt: ...

Następnie zdarzenie trafia do Kafki.

RiskSignalAgent interpretuje sygnał, a CustomerAgent aktualizuje kontekst ryzyka.

Oracle Graph Analysis
          |
          v
SanctionsExposureDetected
          |
          v
        Kafka
          |
          v
    RiskSignalAgent
          |
          v
     CustomerAgent
          |
          v
 CustomerSituationState
          |
          v
    Risk Evaluation

To bardzo dobrze wykorzystuje przewidziany w dokumencie podział odpowiedzialności między RiskSignalAgent i CustomerAgent. ExaPekdoDesign.md

Ważna cecha

Samo istnienie pośredniego powiązania nie oznacza automatycznie naruszenia sankcji.

System powinien rozróżniać:

Powiązanie grafowe — istnieje ścieżka między podmiotami.

Ekspozycja na sankcje — charakter ścieżki i obowiązujące reguły wskazują na możliwą ekspozycję.

Potwierdzone naruszenie — ustalenie wymagające odrębnej oceny prawnej i zgodności.

Korzyść biznesowa

System może wykrywać powiązania, których nie widać podczas sprawdzania wyłącznie bezpośrednich kontrahentów.

Dodatkowo może ponownie oceniać sytuację klienta, gdy:

* na liście pojawi się nowy podmiot;
* zmieni się struktura własności;
* powstanie nowa relacja;
* zostanie wykryta wcześniej nieznana zależność.

Nie trzeba wtedy uruchamiać całej analizy sytuacji klienta od początku.

⸻

5. Potrzeba trzecia: identyfikacja beneficjentów rzeczywistych (UBO)

Wymaganie

Chcemy przejść przez powiązania firm do beneficjentów rzeczywistych — tych, którzy faktycznie kontrolują te firmy i ostatecznie dostają pieniądze — i sprawdzić, czy gdzieś po drodze nie ma czarnych list.

Ten przypadek wymaga bardziej zaawansowanego modelowania niż zwykłe relacje między klientami.

Jak system to realizuje?

Należy rozszerzyć Global Relationship Graph o relacje własności i kontroli.

Przykład:

Company A
    |
    | owned 80%
    v
Company B
    |
    | owned 60%
    v
Company C
    |
    | controlled by
    v
Person X
    |
    | sanctions status
    v
SANCTIONED

System powinien umożliwiać przechodzenie przez struktury właścicielskie aż do osób fizycznych.

Dodatkowo powinien obliczać pośrednie udziały.

Przykładowo:

Company A
    |
    | 80%
    v
Company B
    |
    | 60%
    v
Person X

Pośredni udział ekonomiczny osoby X w Company A wynosi:

0.80 × 0.60 = 0.48

Czyli 48%.

To jednak tylko prosty przypadek. W rzeczywistych strukturach należy uwzględnić wiele równoległych ścieżek własności, możliwe cykle oraz kontrolę niewynikającą bezpośrednio z procentowego udziału.

Rekomendowane rozszerzenie

Wprowadzenie wyspecjalizowanego:

BeneficialOwnershipAnalysis

Odpowiedzialnego za:

1. Analizowanie wielopoziomowych struktur własności.
2. Wyznaczanie pośrednich udziałów.
3. Identyfikowanie osób sprawujących kontrolę.
4. Wykrywanie pośrednich powiązań z listami restrykcyjnymi.
5. Utrzymywanie historii zmian struktury właścicielskiej.

Wynik:

BeneficialOwnershipExposure
companyId: COMPANY-A
ultimateBeneficialOwner:
    PERSON-X
effectiveOwnership: 48%
ownershipPath:
    COMPANY-A
      -> COMPANY-B
      -> PERSON-X
sanctionsExposure: TRUE

Integracja z Pekko

Można wykorzystać RelationshipAgent do utrzymywania kontekstu określonej relacji właścicielskiej lub ekspozycji.

Nie powinien on jednak samodzielnie wykonywać pełnego przechodzenia przez rozległą strukturę przedsiębiorstw.

To zadanie powinno należeć do globalnej warstwy analitycznej Oracle.

Korzyść biznesowa

Możliwość analizy złożonych struktur korporacyjnych i identyfikacji rzeczywistych osób sprawujących kontrolę nad przedsiębiorstwami.

Dodatkową wartością jest możliwość wyjaśnienia wyniku — wskazania konkretnej ścieżki własności i powodu wykrycia ekspozycji.

⸻

6. Potrzeba czwarta: wykrywanie cyklicznych przepływów pieniędzy

Wymaganie

Chcemy patrzeć, czy pieniądze nie krążą w kółko między podmiotami — czy nie chodzi o pranie pieniędzy albo nabijanie sobie dochodu.

To jeden z ciekawszych przypadków, ponieważ pozwala wykorzystać jednocześnie analizę grafową i przetwarzanie strumieniowe.

Przykład

Company A
    |
    | 100 000 EUR
    v
Company B
    |
    | 98 000 EUR
    v
Company C
    |
    | 96 000 EUR
    v
Company A

Z punktu widzenia pojedynczych transakcji każda operacja może wyglądać poprawnie.

Dopiero analiza całej sekwencji pokazuje, że środki wracają do podmiotu początkowego.

Proponowane rozwiązanie

Wykorzystałbym dwa uzupełniające się mechanizmy.

A. Apache Flink — analiza strumieniowa

Flink może analizować sekwencje transakcji w określonych oknach czasowych.

Przykładowo:

A -> B
B -> C
C -> A

Jeżeli przepływy nastąpiły w krótkim czasie i spełniają określone warunki, Flink może wygenerować sygnał:

PotentialCircularFlowDetected

Flink jest do tego naturalnym komponentem, ponieważ dokument wskazuje jego zastosowanie do obsługi czasu zdarzeń, wzbogacania danych i wielostrumieniowych połączeń. ExaPekdoDesign.md

Nie oznacza to jednak, że wykrywanie dowolnych cykli w dużym, dynamicznym grafie będzie proste do realizacji wyłącznie we Flinku.

B. Oracle Property Graph — analiza globalna

Oracle może analizować pełniejszą historię przepływów.

Przykładowe zadania:

* wykrywanie cykli transakcyjnych;
* identyfikowanie powtarzających się sekwencji;
* analiza przepływów przez wiele rachunków;
* identyfikacja grup podmiotów intensywnie przekazujących sobie środki;
* analiza powiązań między przepływami a strukturą właścicielską.

Ważne jest, aby nie traktować każdego cyklu jako dowodu prania pieniędzy. Wykryty wzorzec jest sygnałem wymagającym dalszej oceny.

Integracja

Banking Transactions
         |
         v
       Kafka
         |
         v
       Flink
         |
         +-------------------------+
         |                         |
         v                         v
Streaming Pattern           Oracle Exadata
   Detection                      |
         |                         v
         |                  Global Graph
         |                      Analysis
         |                         |
         +------------+------------+
                      |
                      v
             AML Risk Signal
                      |
                      v
                RiskSignalAgent
                      |
                      v
                 CustomerAgent
                      |
                      v
             Updated Situation

Najważniejsza zaleta

W ten sposób system może łączyć dwa rodzaje wiedzy:

Zachowanie podmiotu w czasie — analizowane na bieżąco przez Flink.

Struktura jego powiązań — analizowana globalnie w Oracle.

To może znacząco poprawić interpretację nietypowych przepływów.

⸻

7. Potrzeba piąta: wspólny graf osób fizycznych i przedsiębiorstw

Wymaganie

Chcemy łapać styki między firmami a osobami fizycznymi, bo te powiązania też istnieją i chcielibyśmy je oglądać z różnych perspektyw.

To wymaganie dotyczy przede wszystkim modelu danych grafowych.

Rekomendacja

Zamiast budować osobne grafy klientów indywidualnych i korporacyjnych, proponuję jeden heterogeniczny graf relacji.

Powinien obejmować różne typy węzłów:

PERSON
COMPANY
ACCOUNT
TRANSACTION
DOCUMENT
ADDRESS

Oraz relacje, takie jak:

OWNS
CONTROLS
BENEFICIAL_OWNER_OF
DIRECTOR_OF
AUTHORIZED_SIGNATORY_OF
HOLDS_ACCOUNT
TRANSFERRED_TO
RELATED_TO

Przykład:

               Person A
                  |
             owns 45%
                  |
                  v
               Company X
                  |
             owns 100%
                  |
                  v
               Company Y
                  |
             holds account
                  |
                  v
               Account Z
                  |
             transfers to
                  |
                  v
               Person B

Taki graf pozwala spojrzeć na te same dane z różnych perspektyw.

Perspektywa osoby

Jakie przedsiębiorstwa kontroluje dana osoba?

Perspektywa firmy

Kto jest jej beneficjentem rzeczywistym?

Perspektywa transakcyjna

Przez jakie rachunki i podmioty przepływają środki?

Perspektywa ryzyka

Czy osoba lub firma jest pośrednio powiązana z podmiotami wysokiego ryzyka?

Perspektywa sieciowa

Czy pozornie niezależne przedsiębiorstwa mają wspólnych właścicieli, dyrektorów albo beneficjentów?

To rozwinięcie koncepcji Global Relationship Graph, którą dokument już przewiduje. ExaPekdoDesign.md

Warto oddzielić graf własności i powiązań od grafu przepływów finansowych na poziomie logicznym, nawet jeżeli oba są dostępne w ramach jednej platformy. Pozwala to zachować właściwą semantykę relacji i uruchamiać analizy łączące oba wymiary.

⸻

8. Co należałoby rozszerzyć w obecnej architekturze?

Dokument zawiera solidną podstawę, ale przedstawione potrzeby uzasadniają wprowadzenie dodatkowej warstwy.

Proponuję nazwać ją:

Global Graph Intelligence

Byłaby ona częścią Global Situation Intelligence działającej na danych Oracle Exadata.

Jej głównym zadaniem byłaby zamiana surowych relacji grafowych w interpretowalne fakty i sygnały biznesowe.

Proponowana architektura logiczna

                    BANKING SYSTEMS
                          |
                          v
                        Kafka
                          |
                          v
                    Apache Flink
                          |
             +------------+------------+
             |                         |
             v                         v
       Apache Pekko              Oracle Exadata
             |                         |
             |                  +------+------+
             |                  |             |
             |                Facts       Property Graph
             |                                |
             |                                v
             |                      GLOBAL GRAPH INTELLIGENCE
             |                                |
             |                    +-----------+-----------+
             |                    |           |           |
             |                Network      Sanctions     UBO
             |                Features     Exposure    Analysis
             |                    |           |           |
             |                    +-----------+-----------+
             |                                |
             |                          AML Patterns
             |                                |
             |                                v
             |                       Intelligence Events
             |                                |
             |                                v
             +<----------------------------- Kafka
             |
             v
      +--------------------+
      |    Pekko Agents    |
      +--------------------+
      | CustomerAgent      |
      | RelationshipAgent  |
      | RiskSignalAgent    |
      | PortfolioRiskAgent |
      +--------------------+
             |
             v
       Situation State
             |
             v
      Situation Evaluation

To schemat proponowanego rozszerzenia, a nie element już w pełni opisany w dokumencie.

Pięć obszarów Global Graph Intelligence

Komponent	Zadanie
Network Feature Computation	Obliczanie cech otoczenia klienta dla modeli ML
Sanctions Exposure Analysis	Wykrywanie pośrednich powiązań z listami restrykcyjnymi
Beneficial Ownership Analysis	Identyfikacja UBO oraz struktur kontroli
Circular Flow Detection	Wykrywanie podejrzanych cykli finansowych
Relationship Exploration	Udostępnianie różnych perspektyw na sieć relacji

Istotne jest, że nie wszystkie funkcje muszą być realizowane w ten sam sposób.

Cechy sieciowe można wyliczać okresowo.

Ekspozycja na sankcje może wymagać ponownej analizy po zmianie listy restrykcyjnej.

Wykrywanie cyklicznych przepływów może łączyć przetwarzanie strumieniowe z analizą historyczną.

Eksploracja relacji może być wykonywana interaktywnie, na żądanie użytkownika.

⸻

9. Największa wartość: połączenie grafu z Situation Awareness

Moim zdaniem właśnie tutaj przedstawiona architektura ma największy potencjał.

Rozważmy przykład.

Bank obsługuje klienta A.

Jego sytuacja finansowa wygląda stabilnie:

CustomerSituation(A)
incomeStability: HIGH
liquidityRisk: LOW
paymentBehavior: NORMAL

Następnie Oracle odkrywa nowe powiązanie:

Customer A
    |
    v
Company B
    |
    v
Company C
    |
    v
Sanctioned Person

Global Graph Intelligence generuje zdarzenie:

SanctionsExposureDetected

Zdarzenie trafia do CustomerAgent(A).

Agent aktualizuje stan:

CustomerSituation(A)
incomeStability: HIGH
liquidityRisk: LOW
paymentBehavior: NORMAL
networkRisk:
    sanctionsExposure: TRUE
    exposureDistance: 3
    evidenceId: GRAPH-123

Wynik jest niezwykle istotny.

Sytuacja finansowa klienta się nie zmieniła, ale zmieniła się wiedza banku o ryzyku związanym z tym klientem.

Dlatego zmienia się jego sytuacja w rozumieniu systemu.

To dokładnie odpowiada filozofii Situation Awareness opisanej w dokumencie.

W modelu Endsley system przechodzi przez trzy poziomy:

Poziom	Interpretacja
Perception	Wykryto powiązanie klienta z podmiotem objętym sankcjami
Comprehension	Zinterpretowano znaczenie tego powiązania dla sytuacji klienta
Projection	Oceniono potencjalne konsekwencje dla przyszłej ekspozycji i procesów bankowych

Dokument opisuje wszystkie trzy poziomy jako fundament systemu. ExaPekdoDesign.md

⸻

10. Na co trzeba szczególnie uważać?

Widzę pięć zagadnień architektonicznych, które wymagają dodatkowego projektu.

1. Rozmiar grafu i wieloskokowe zapytania

Wyszukiwanie relacji na kilka poziomów może być kosztowne przy dużej liczbie węzłów i krawędzi.

Nie należy zakładać, że dowolne przejście przez graf będzie mogło być wykonywane synchronicznie podczas obsługi transakcji.

Należy rozdzielić analizy okresowe, przyrostowe i interaktywne oraz przetestować ich wydajność na docelowej konfiguracji Oracle.

2. Semantyka relacji

Nie wszystkie ścieżki mają takie samo znaczenie.

Przykładowo:

Person A → owns → Company B

to zupełnie inny rodzaj powiązania niż:

Person A → transferred_money_to → Company B.

Algorytmy sankcyjne i UBO powinny uwzględniać typy relacji, a nie wyłącznie liczbę skoków.

3. Historyczność grafu

Struktury właścicielskie zmieniają się w czasie.

Podobnie zmieniają się statusy sankcyjne.

Graf powinien pozwalać na odtworzenie odpowiedzi na pytanie:

Jak wyglądała wiedza banku o powiązaniach klienta w momencie podejmowania konkretnej decyzji?

Wymaga to wersjonowania danych, rozróżniania czasu obowiązywania relacji i czasu pozyskania informacji.

4. Jakość identyfikacji podmiotów

Osoba występująca w kilku systemach nie zawsze ma ten sam identyfikator.

Przed zbudowaniem wiarygodnego grafu trzeba rozwiązać problem identyfikacji i łączenia rekordów podmiotów (entity resolution).

Bez tego system może zarówno przeoczać rzeczywiste powiązania, jak i tworzyć fałszywe ścieżki.

5. Wyjaśnialność i audyt

Każdy sygnał powinien zawierać:

* identyfikator podmiotu;
* typ wykrytego wzorca;
* ścieżkę relacji stanowiącą dowód;
* czas obowiązywania analizowanych danych;
* wersję reguł lub modelu;
* ocenę jakości i kompletności danych.

To zgodne z zasadą przedstawioną w dokumencie, zgodnie z którą każda ewaluacja powinna być powiązana z konkretną wersją stanu, reguł i danych. ExaPekdoDesign.md

⸻

11. Jak proponuję zweryfikować tę koncepcję z klientem?

Nie zaczynałbym od implementacji wszystkich pięciu scenariuszy jednocześnie.

Proponuję trzy etapy.

Etap	Zakres	Co chcemy udowodnić?
PoC 1 — Graph Context	Heterogeniczny graf osób i firm, analizy 2–3 skoków, wyliczanie cech sieciowych	Czy graf dostarcza wiarygodnych i przydatnych cech?
PoC 2 — Compliance Intelligence	Powiązania sankcyjne, analiza UBO, interpretacja ścieżek	Czy można wykrywać i wyjaśniać pośrednie ekspozycje?
PoC 3 — Transaction Network Intelligence	Cykliczne przepływy, analiza transakcji i integracja z Pekko	Czy system wykrywa istotne wzorce i aktualizuje sytuację klienta?

W pierwszym PoC warto dodatkowo porównać istniejący model ML z modelem wzbogaconym o cechy grafowe. Kluczowym wynikiem powinien być rzeczywisty przyrost jakości modelu, a nie tylko możliwość obliczenia nowych wskaźników.

Dopiero po sprawdzeniu jakości danych, wyników algorytmów i wydajności Oracle należy ustalić, które analizy powinny być uruchamiane w czasie rzeczywistym.

⸻

12. Ostateczna rekomendacja

Nie budowałbym osobnego systemu analizy grafowej obok Banking Situation Awareness. Rozszerzyłbym istniejącą architekturę o Global Graph Intelligence, stanowiącą integralną część Global Situation Intelligence.

To pozwala wykorzystać wszystkie komponenty zgodnie z ich naturalnymi odpowiedzialnościami:

Oracle Exadata przechowuje globalny graf relacji, fakty i historię oraz wykonuje analizy obejmujące wielu klientów.

Apache Flink analizuje strumienie transakcji i wykrywa wzorce, które można rozpoznać na podstawie zdarzeń napływających w czasie.

Apache Pekko interpretuje znaczenie uzyskanych informacji dla bieżącej sytuacji konkretnego klienta.

Kafka zapewnia asynchroniczne przekazywanie zdarzeń pomiędzy warstwami.

Co szczególnie ważne, dzięki takiemu podejściu wynik analizy grafowej nie jest jedynie odpowiedzią na zapytanie.

Staje się nową wiedzą, która może trwale zmienić sposób, w jaki system rozumie sytuację klienta.

Najważniejszy wniosek

Wszystkie pięć wymagań klienta można sprowadzić do trzech podstawowych zdolności systemu:

Zdolność	Pytanie, na które odpowiada
Graph Context Awareness	Co możemy powiedzieć o kliencie na podstawie jego otoczenia?
Graph Risk Intelligence	Jakie ryzyka wynikają z bezpośrednich i pośrednich powiązań klienta?
Transaction Network Intelligence	Jakie wzorce przepływu pieniędzy występują w sieci powiązań klienta?

Każda z tych zdolności zasila nadrzędny mechanizm Customer Situation Awareness.

W ten sposób proponowana platforma przestaje być wyłącznie systemem analizy sytuacji finansowej klienta.

Staje się systemem rozumienia sytuacji klienta w kontekście całej sieci bankowej — jego finansów, zachowania, relacji, własności i ekspozycji na ryzyko.

I to właśnie jest najważniejsza wartość biznesowa proponowanej architektury.
