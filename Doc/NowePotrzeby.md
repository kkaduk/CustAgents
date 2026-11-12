# **Analiza realizacji potrzeb biznesowych w architekturze ARCHITECTURE**

## **1. Podsumowanie**

Architektura opisana w dokumencie **ARCHITECTURE** stanowi dobrą podstawę do realizacji przedstawionych scenariuszy wykrywania nadużyć, analizy powiązań i przeciwdziałania wyprowadzaniu środków.

Jej najważniejszą cechą jest połączenie dwóch mechanizmów:

- **Local Situation Intelligence (Apache Pekko)** – bieżąca wiedza o kliencie, jego transakcjach, relacjach i rsyzykach, aktualizowana na podstawie napływających zdarzeń.
- **Global Situation Intelligence (Oracle Exadata Property Graph)** – analiza powiązań między klientami, rachunkami i kontrahentami w skali całego banku, pozwalająca wykrywać wzorce niewidoczne z perspektywy pojedynczego klienta.

Apache Kafka i Apache Flink odpowiadają za dostarczanie oraz wzbogacanie zdarzeń, natomiast mechanizm `EventPluginRegistry` pozwala rozszerzać system o nowe rodzaje zdarzeń i reguły interpretacji.

**Istotne rozróżnienie:** obecna implementacja koncentruje się na analizie transakcji, ryzyka sieciowego oraz decyzjach kredytowych. Nie wszystkie opisane potrzeby fraudowe są już zaimplementowane. Część można obsłużyć istniejącymi mechanizmami, a część wymaga rozszerzenia modelu danych, grafu i logiki decyzyjnej.

ARCHITECTURE(1).md

### **Ocena pokrycia wymagań**

|**Potrzeba biznesowa**|**Pokrycie przez architekturę**|**Co należy rozszerzyć**|
|---|---|---|
|Wykrywanie powiązań pracownika z klientem|🟡 Częściowe – istnieje infrastruktura grafowa|Model pracownika, telefonu i historii zmian danych|
|Wykrywanie kont słupów|🟡 Częściowe – istnieje analiza grafowa i propagacja ryzyka|Dedykowane wzorce wykrywania słupów|
|Analiza przepływów między rachunkami własnymi|🟡 Częściowe – istnieje rejestracja transakcji i analiza klienta|Model rachunków i własności rachunków|
|Sprawdzenie historii rachunku przy przelewie 1 PLN|🟡 Możliwe na istniejącej infrastrukturze|Rejestr pierwszego wystąpienia rachunku i dedykowany mechanizm zapytań|
|Interaktywna analiza powiązań przez analityka|🟡 Częściowe – istnieje graf i API sytuacji klienta|Interfejs eksploracji grafu i szczegółów transakcji|
|Blokowanie podejrzanych transakcji|🔴 Niezaimplementowane|Mechanizm oceny przed wykonaniem przelewu i integracja z systemem płatniczym|

---

# **2. Wykrywanie powiązań pracownika z obsługiwanym klientem**

### **Potrzeba biznesowa**

Bank chce identyfikować potencjalnie nieuprawnione powiązania między pracownikiem a klientem.

Przykładowy scenariusz:

1. Pracownik banku posiada numer telefonu `500 100 200`.
2. Pracownik obsługuje klienta A.
3. W kartotece klienta A następuje zmiana numeru telefonu.
4. Nowy numer klienta jest identyczny z numerem pracownika.
5. System wykrywa powiązanie i generuje sygnał ryzyka.

### **Jak wspiera to obecna architektura?**

**Podstawowym mechanizmem jest Oracle Exadata Property Graph (****`SA_GRAPH`****).**

W obecnej architekturze graf przechowuje:

- wierzchołki `GRAPH_VERTICES`,
- krawędzie `GRAPH_EDGES`,
- relacje między klientami i kontrahentami,
- informacje o ryzyku związanym z poszczególnymi podmiotami.

Dodatkowo `CustomerAgent` utrzymuje lokalną sieć powiązań klienta (`egoNetwork`).

Obecnie jednak graf koncentruje się na relacjach transakcyjnych między `BANK_CUSTOMER`, `EXTERNAL_ENTITY` i `RESTRICTED_ENTITY`.

Nie zawiera jeszcze pełnego modelu pracowników, telefonów ani historii zmian danych kontaktowych.

ARCHITECTURE(1).md

### **Proponowana realizacja**

Należy rozszerzyć graf o nowe typy wierzchołków:

|**Typ wierzchołka**|**Znaczenie**|
|---|---|
|`EMPLOYEE`|Pracownik banku|
|`BANK_CUSTOMER`|Klient banku|
|`PHONE_NUMBER`|Numer telefonu|
|`CUSTOMER_ACCOUNT`|Rachunek bankowy|

Oraz relacje:

|**Relacja**|**Znaczenie**|
|---|---|
|`EMPLOYEE_HAS_PHONE`|Numer telefonu pracownika|
|`CUSTOMER_HAS_PHONE`|Numer telefonu przypisany do klienta|
|`EMPLOYEE_SERVICES_CUSTOMER`|Pracownik obsługuje klienta|
|`CUSTOMER_PHONE_CHANGED`|Zmiana numeru telefonu klienta|

Dla relacji należy przechowywać historię obowiązywania, źródło informacji i identyfikator zdarzenia.

**Przebieg wykrywania:**

1. Zmiana numeru telefonu klienta trafia do Kafka jako nowe zdarzenie.
2. Flink normalizuje numer telefonu.
3. Zdarzenie aktualizuje relacje w `SA_GRAPH`.
4. Analiza grafowa identyfikuje pracowników posiadających ten sam numer.
5. System sprawdza, czy dany pracownik obsługuje tego klienta.
6. W przypadku wykrycia powiązania powstaje zdarzenie ryzyka.
7. `CustomerAgent` aktualizuje sytuację klienta.

Do obsługi zdarzenia można wykorzystać istniejący mechanizm `EventPluginRegistry`, dodając np. `CustomerContactChangedPlugin`.

### **Rezultat biznesowy**

System pozwoli wykryć sytuację:

**Pracownik → numer telefonu → klient obsługiwany przez tego pracownika.**

Analityk otrzyma informację o powiązaniu wraz z historią zmiany, a nie tylko informację, że numery są identyczne.

Samo współdzielenie numeru powinno być traktowane jako sygnał wymagający wyjaśnienia, a nie dowód nadużycia.

**Status: wymaga rozszerzenia modelu grafowego i obsługi zdarzeń dotyczących danych kontaktowych.**

---

# **3. Wykrywanie kont słupów i analiza przepływów pieniędzy**

### **Potrzeba biznesowa**

Bank chce wykrywać rachunki wykorzystywane do pośredniczenia w transferowaniu środków.

Przykładowy scenariusz:

Klient A przelewa 50 000 PLN do klienta B. Klient B natychmiast przekazuje 49 500 PLN do klienta C, a następnie środki trafiają na rachunek zewnętrzny.

Bank chce ustalić:

- skąd pochodzą pieniądze,
- przez jakie rachunki przepływają,
- kto jest właścicielem poszczególnych rachunków,
- czy występują podejrzane wzorce,
- czy uczestnicy przepływu są powiązani z innymi przypadkami nadużyć.

## **3.1. Analiza lokalna – CustomerAgent**

W obecnej architekturze każdy klient posiada własnego `CustomerAgent`, który na bieżąco aktualizuje:

- wpływy i wypływy,
- liczbę transakcji,
- historię kontaktów z kontrahentami,
- ekspozycję na ryzyko,
- wskaźniki przepływów finansowych.

Mechanizm `SlidingWindowCashflow` pozwala analizować transakcje w oknach czasowych obejmujących maksymalnie 90 dni.

Dzięki temu system może szybko identyfikować nietypowe zmiany aktywności klienta, np. nagły wzrost wartości przelewów.

ARCHITECTURE(1).md

Jednak obecne agregaty nie stanowią jeszcze gotowego detektora kont słupów.

## **3.2. Analiza globalna – Oracle Property Graph**

To najważniejszy element realizacji tego wymagania.

Każda transakcja jest zapisywana w:

`SA_FACTS.FINANCIAL_TRANSACTION_FACTS`

i odwzorowywana jako relacja w:

`SA_GRAPH.GRAPH_EDGES`.

System posiada mechanizm `analyzeGlobalGraphAndDetectPatterns`, który identyfikuje klientów korzystających ze wspólnych kontrahentów wysokiego ryzyka.

W obecnej implementacji analizowane są między innymi przypadki, gdy co najmniej dwóch klientów kieruje środki do wspólnego podmiotu o wysokim ryzyku lub odpowiednio dużym wolumenie transakcji.

Wykrycie takiej struktury generuje `NetworkRiskPatternDetected`.

ARCHITECTURE(1).md

### **Co trzeba rozszerzyć?**

Należy dodać dedykowane wzorce analityczne, np.:

**Rapid Pass-Through**

Rachunek otrzymuje środki, po czym w krótkim czasie przekazuje znaczną ich część dalej.

**Fan-In / Fan-Out**

Wiele rachunków przekazuje pieniądze na jeden rachunek, który następnie rozsyła środki do wielu odbiorców.

**Multi-Hop Money Movement**

Środki przemieszczają się przez kolejne rachunki, tworząc łańcuch transferów.

**Circular Transfers**

Pieniądze wracają do wcześniejszego uczestnika przepływu.

Wszystkie te mechanizmy można oprzeć na Oracle Property Graph, ale nie są one opisane jako zaimplementowane detektory w dokumencie ARCHITECTURE.

## **3.3. Przepływy między własnymi rachunkami**

To wymaganie wymaga istotnego rozszerzenia modelu.

Obecna architektura utrzymuje sytuację przede wszystkim na poziomie `customerId`.

Nie posiada jawnego modelu:

`Customer → Account → Transaction → Account → Customer`

Dlatego nie rozróżnia w pełni przelewów między własnymi rachunkami od przelewów między różnymi właścicielami.

### **Proponowane rozwiązanie**

Wprowadzenie w grafie wierzchołków `BANK_ACCOUNT` i relacji `CUSTOMER_OWNS_ACCOUNT`.

Przykład:

```text
              KLIENT A
                 |
        +--------+--------+
        |                 |
     Konto 1           Konto 2
        |                 |
        +---- 20 000 -----+
                          |
                       15 000
                          |
                       Konto 3
                          |
                       KLIENT B
```

System będzie mógł rozróżnić:

- transfer 20 000 PLN między własnymi rachunkami klienta A,
- rzeczywisty transfer 15 000 PLN do klienta B.

To ważne również dla agregatów finansowych: przelewy wewnętrzne nie powinny automatycznie zawyżać rzeczywistych przychodów ani wydatków klienta.

**Status: analiza grafowa jest dostępna, ale pełne śledzenie przepływów na poziomie rachunków i dedykowane wykrywanie słupów wymagają rozbudowy.**

---

# **4. Weryfikacja rachunku przy przelewie na 1 PLN**

### **Potrzeba biznesowa**

Podczas przelewu weryfikacyjnego bank chce sprawdzić:

1. Czy rachunek odbiorcy był wcześniej znany?
2. Kiedy pojawił się po raz pierwszy?
3. Z jakimi klientami był wcześniej powiązany?
4. Czy występował w podejrzanych transakcjach?

Brak wcześniejszej historii rachunku jest sygnałem ryzyka kradzieży tożsamości.

Nie oznacza jednak automatycznej odmowy wykonania transakcji.

## **4.1. Wykorzystanie Oracle Exadata**

Architektura przechowuje historię transakcji w `SA_FACTS` oraz powiązania w `SA_GRAPH`.

Dzięki temu istnieje podstawa do realizacji zapytań historycznych.

Obecny model nie przechowuje jednak jawnie numeru rachunku ani daty jego pierwszego wystąpienia.

Dlatego konieczne jest rozszerzenie danych o:

- identyfikator rachunku,
- datę pierwszego zaobserwowania,
- datę ostatniego wystąpienia,
- liczbę transakcji,
- historię powiązań z klientami.

### **Proponowany mechanizm**

Przy pierwszym zaobserwowaniu rachunku system zapisuje:

`AccountFirstSeen`

Przy kolejnych transakcjach aktualizuje jego historię.

Przykładowy wynik sprawdzenia:

|**Atrybut**|**Wartość**|
|---|---|
|Rachunek|PLxx…1234|
|Znany w banku|TAK|
|Pierwsze wystąpienie|2023-04-15|
|Ostatnie wystąpienie|2026-11-10|
|Liczba transakcji|127|
|Powiązania wysokiego ryzyka|BRAK|

Dla rachunku nieznanego:

|**Atrybut**|**Wartość**|
|---|---|
|Znany w banku|NIE|
|Pierwsze wystąpienie|Brak w dostępnej historii|
|Ocena|Nowy / nieznany rachunek|
|Sygnał ryzyka|`UNKNOWN_ACCOUNT_HISTORY`|

Ważne, aby rozróżniać **rachunek nowy** od **rachunku, którego bank wcześniej nie obserwował**. Brak historii w systemie nie dowodzi daty utworzenia rachunku.

## **4.2. Wykorzystanie Local Situation Intelligence**

Wynik sprawdzenia może zostać przekazany do `CustomerAgent` jako nowe zdarzenie.

Przykładowo:

`AccountHistoryRiskDetected`

Dedykowany plugin aktualizuje sytuację klienta, uwzględniając nowy sygnał ryzyka.

Dzięki temu informacja może być wykorzystana również w kolejnych operacjach.

**Status: mechanizm możliwy do realizacji na istniejącej architekturze, ale wymaga nowego modelu rachunku, historii jego występowania oraz interfejsu weryfikacyjnego.**

---

# **5. Interaktywna analiza powiązań przez analityka**

### **Potrzeba biznesowa**

Analityk chce rozpocząć analizę od konkretnego klienta lub firmy, a następnie samodzielnie eksplorować sieć powiązań.

Powinien mieć możliwość przechodzenia od ogólnego obrazu do szczegółów pojedynczych transakcji.

## **5.1. Co zapewnia obecna architektura?**

System udostępnia trzy istotne źródła informacji.

**1. CustomerAgent**

Przechowuje bieżącą sytuację klienta, w tym:

- `egoNetwork`,
- `riskExposures`,
- `CustomerSituationSummary`.

**2. Oracle Property Graph**

Przechowuje globalną sieć powiązań między klientami i kontrahentami.

**3. SA_FACTS**

Przechowuje historię poszczególnych transakcji.

Architektura udostępnia również API:

`GET /api/v1/customers/{id}/situation`

oraz interfejs `Interactive Business Workbench`.

ARCHITECTURE(1).md

Nie oznacza to jednak, że obecny Workbench realizuje pełną interaktywną eksplorację grafu.

## **5.2. Proponowany sposób realizacji**

Analityk rozpoczyna od klienta:

```text
KLIENT A
   |
   +-- Konto 1
   |     |
   |     +-- Transakcja 1001
   |     |      |
   |     |      +-- KLIENT B
   |     |
   |     +-- Transakcja 1002
   |            |
   |            +-- KLIENT C
   |
   +-- Telefon 500...
   |      |
   |      +-- PRACOWNIK X
   |
   +-- Konto 2
          |
          +-- Transakcja 1003
                 |
                 +-- KLIENT D
```

Analityk powinien móc:

1. Wybrać klienta lub firmę.
2. Wyświetlić jego bezpośrednie powiązania.
3. Rozwinąć powiązania o kolejne poziomy.
4. Filtrować relacje według rodzaju i czasu.
5. Zidentyfikować wspólne telefony, rachunki lub kontrahentów.
6. Przejść do pojedynczej transakcji.
7. Zobaczyć powód wygenerowania sygnału ryzyka.

### **Ważna cecha architektury**

**Analityczne zapytania grafowe nie muszą obciążać bieżącego przetwarzania transakcji.**

Zgodnie z ARCHITECTURE Oracle Exadata odpowiada za globalną analizę powiązań, natomiast Pekko utrzymuje bieżący stan klientów.

Dodatkowo dokument opisuje wykorzystanie natywnego `GRAPH_TABLE` w Oracle 23ai/26ai do wykonywania analizy grafowej bezpośrednio w bazie danych.

ARCHITECTURE(1).md

To pozwala wykorzystać wspólną bazę danych zarówno do automatycznego wykrywania wzorców, jak i do analiz prowadzonych przez człowieka.

**Status: warstwa danych i mechanizm analizy grafowej istnieją; potrzebne jest rozszerzenie API i interfejsu analitycznego.**

---

# **6. Blokowanie podejrzanych transakcji**

### **Potrzeba biznesowa**

Bank chce zatrzymać wyprowadzanie pieniędzy, zanim transakcja zostanie wykonana.

Podstawą oceny mogą być:

- nowy lub nieznany rachunek,
- podejrzenie wykorzystania konta słupa,
- obecność kontrahenta na liście restrykcyjnej,
- podejrzane powiązanie telefoniczne z pracownikiem.

Progi blokowania nie zostały jeszcze określone.

## **6.1. Co już realizuje architektura?**

Architektura posiada mechanizm oceny ryzyka w czasie rzeczywistym.

Apache Flink wzbogaca transakcje o informacje z list restrykcyjnych, wykorzystując `WatchlistBroadcastEnricher`.

Następnie `CustomerAgent` aktualizuje lokalny stan ryzyka klienta.

Dodatkowo `RiskSignalAgent` propaguje informacje o ryzyku między powiązanymi klientami.

Przykładowo, jeżeli klient B realizuje transakcję z podmiotem objętym sankcjami, informacja o ryzyku może zostać przekazana do powiązanego klienta A.

ARCHITECTURE(1).md

System posiada także `ExplainableCreditEngine`, który wykonuje deterministyczne reguły decyzyjne.

Jednak obecnie silnik ten służy do **oceny wniosków kredytowych**, a nie autoryzacji przelewów.

Nie należy więc utożsamiać istniejącej decyzji kredytowej z mechanizmem blokowania płatności.

## **6.2. Proponowane rozszerzenie – Transaction Risk Decision Engine**

Należy wprowadzić nowy mechanizm oceny transakcji przed jej realizacją.

Proponowany przebieg:

```text
ZLECENIE PRZELEWU
        |
        v
Transaction Risk Evaluation
        |
        +-- Czy rachunek jest znany?
        |
        +-- Czy odbiorca jest podejrzanym słupem?
        |
        +-- Czy występuje wpis restrykcyjny?
        |
        +-- Czy istnieje podejrzane powiązanie?
        |
        v
CustomerAgent
Bieżąca sytuacja klienta
        |
        v
Transaction Risk Decision Engine
        |
        +-- ALLOW
        |
        +-- REVIEW / HOLD
        |
        +-- BLOCK
        |
        v
SYSTEM PŁATNICZY
```

### **Kluczowa zasada**

Decyzja powinna wykorzystywać **wcześniej przygotowany stan ryzyka**, a nie wykonywać pełną analizę grafową od początku przy każdym przelewie.

To bezpośrednio odpowiada podstawowej filozofii ARCHITECTURE:

- Oracle Exadata identyfikuje złożone powiązania i wzorce.
- Wyniki są przekazywane do `CustomerAgent`.
- Pekko utrzymuje aktualną sytuację klienta.
- Silnik decyzyjny wykorzystuje ten stan podczas oceny transakcji.

Dzięki temu potencjalnie kosztowna analiza grafowa jest oddzielona od ścieżki autoryzacji płatności.

## **6.3. Wyjaśnialność decyzji**

Istniejący model `ReasonCode`, `RuleEvaluationTrace` i `ExplainableAuditTrail` stanowi podstawę do budowy wyjaśnialnych decyzji transakcyjnych.

Przykładowo:

|**Sygnał**|**Przykładowy kod**|**Interpretacja**|
|---|---|---|
|Nieznany rachunek|`UNKNOWN_ACCOUNT_HISTORY`|Brak wcześniejszej historii rachunku|
|Podejrzenie słupa|`SUSPECTED_MULE_ACCOUNT`|Podejrzany wzorzec przepływów|
|Wpis restrykcyjny|`RESTRICTED_COUNTERPARTY`|Powiązanie z listą restrykcyjną|
|Powiązanie telefonu|`EMPLOYEE_CUSTOMER_PHONE_MATCH`|Wspólny numer telefonu pracownika i klienta|

Są to **proponowane kody**, a nie istniejące kody zaimplementowane w ARCHITECTURE.

Sygnały mogą być łączone w ocenę ryzyka, ale sposób ich agregacji i konsekwencje decyzyjne wymagają uzgodnienia z bankiem.

W szczególności brak wcześniejszej historii rachunku nie powinien automatycznie oznaczać blokady.

### **Dodatkowy warunek architektoniczny**

W obecnej architekturze podstawowym zdarzeniem jest transakcja już zaobserwowana (`TransactionObserved`).

Aby blokować przelewy, konieczne jest wprowadzenie osobnej ścieżki **oceny transakcji przed wykonaniem**.

Musi ona uwzględniać między innymi:

- czas odpowiedzi,
- aktualność danych o ryzyku,
- zachowanie przy niedostępności komponentów,
- jednoznaczne przekazanie decyzji do systemu płatniczego,
- audyt podjętej decyzji.

**Status: istnieją komponenty umożliwiające budowę mechanizmu, ale sam mechanizm blokowania przelewów nie jest zaimplementowany.**

---

# **7. Jak wszystkie scenariusze współpracują w jednej architekturze?**

Najważniejszą zaletą ARCHITECTURE jest możliwość wykorzystania tych samych informacji w wielu procesach.

Przykładowo:

**Krok 1 – wykrycie powiązania**

System identyfikuje, że numer telefonu pracownika pojawił się w kartotece obsługiwanego klienta.

Powstaje sygnał ryzyka.

**Krok 2 – analiza przepływów**

Oracle Property Graph wykrywa, że klient przekazuje środki do rachunku uczestniczącego w podejrzanym łańcuchu transferów.

Powstaje kolejny sygnał.

**Krok 3 – aktualizacja sytuacji klienta**

Oba sygnały trafiają do `CustomerAgent`, który utrzymuje bieżący obraz ryzyka.

**Krok 4 – analiza przez człowieka**

Analityk otwiera klienta w Workbench, przegląda powiązania i schodzi do konkretnych transakcji.

**Krok 5 – kolejny przelew**

Klient zleca nowy przelew na rachunek, którego wcześniej nie obserwowano.

Przyszły `Transaction Risk Decision Engine` wykorzystuje zgromadzone sygnały i ocenia, czy transakcję dopuścić, skierować do weryfikacji, czy zablokować.

W ten sposób poszczególne scenariusze nie funkcjonują jako odrębne systemy, lecz wykorzystują wspólną wiedzę o klientach, rachunkach, relacjach i ryzyku.

---

# **8. Rekomendowane rozszerzenia architektury**

Aby w pełni zrealizować przedstawione potrzeby, proponuję sześć rozszerzeń.

|**Priorytet**|**Rozszerzenie**|**Główne komponenty**|**Uzasadnienie**|
|---|---|---|---|
|**P1**|Model rachunków i ich właścicieli|`SA_GRAPH`, `SA_FACTS`|Fundament analizy przepływów i historii rachunków|
|**P1**|Historia pierwszego wystąpienia rachunku|Oracle Exadata, API|Obsługa przelewów weryfikacyjnych|
|**P1**|Rozszerzenie grafu o pracowników i telefony|`SA_GRAPH`, Kafka, Flink|Wykrywanie powiązań pracownik–klient|
|**P2**|Detektory kont słupów|Oracle Property Graph, `SA_INTELLIGENCE`|Identyfikacja podejrzanych wzorców transferów|
|**P2**|Interaktywny Graph Investigation Workbench|API, `SA_GRAPH`, `SA_FACTS`|Samodzielna analiza przypadków przez analityków|
|**P3**|Transaction Risk Decision Engine|Pekko, `SA_DECISIONS`, system płatniczy|Ocena i potencjalne zatrzymywanie transakcji przed realizacją|

Priorytety oznaczają proponowaną kolejność prac, a nie priorytety określone w dokumencie.

## **9. Wnioski końcowe**

Architektura ARCHITECTURE dostarcza większość podstawowych mechanizmów technicznych potrzebnych do realizacji przedstawionych scenariuszy:

**1. Oracle Exadata Property Graph** umożliwia analizę globalnych powiązań i stanowi fundament wykrywania kont słupów, wspólnych numerów telefonów oraz wieloetapowych przepływów finansowych.

**2. Apache Pekko CustomerAgent** utrzymuje bieżącą sytuację klienta, dzięki czemu wykryte wcześniej ryzyka mogą być szybko wykorzystane podczas kolejnych operacji.

**3. Apache Kafka i Apache Flink** zapewniają przetwarzanie zdarzeń, które mogą dotyczyć zarówno transakcji, jak i zmian danych klientów.

**4. EventPluginRegistry** pozwala wprowadzać nowe rodzaje sygnałów ryzyka bez przebudowy podstawowego mechanizmu obsługi zdarzeń.

**5. SA_FACTS i SA_DECISIONS** zapewniają podstawę do odtwarzania historii zdarzeń oraz dokumentowania powodów decyzji.

### **Najważniejsza luka**

Obecny system jest zorientowany na **klienta i jego transakcje**, podczas gdy przedstawione wymagania wymagają bardziej szczegółowego modelu:

**Klient – Rachunek – Transakcja – Kontrahent – Telefon – Pracownik – Powiązanie – Sygnał ryzyka.**

Rozszerzenie tego modelu pozwoli wykorzystać istniejące mechanizmy analityczne bez konieczności projektowania całej platformy od początku.

### **Ostateczna ocena**

**ARCHITECTURE jest odpowiednią bazą architektoniczną dla przedstawionych potrzeb, ale nie stanowi jeszcze kompletnego rozwiązania fraudowego.**

Największa część prac dotyczy rozszerzenia modelu danych i reguł wykrywania ryzyka. Osobnym, istotnym przedsięwzięciem będzie budowa mechanizmu oceny i zatrzymywania transakcji przed ich wykonaniem.

Kluczowe jest zachowanie obecnego podziału odpowiedzialności:

**Oracle Exadata – odkrywanie i analiza powiązań.**

**Apache Pekko – bieżąca wiedza o sytuacji klienta.**

**Kafka/Flink – przetwarzanie nowych zdarzeń.**

**Decision Engine – szybka, wyjaśnialna decyzja operacyjna.**

Taki podział umożliwia obsługę zarówno analiz prowadzonych przez człowieka, jak i przyszłych automatycznych mechanizmów przeciwdziałania nadużyciom.