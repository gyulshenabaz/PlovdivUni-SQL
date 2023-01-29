CREATE DATABASE OnlineShop;

GO

USE OnlineShop;

GO

/* Модели */

CREATE TABLE Customers (
    Id INT IDENTITY(1,1) PRIMARY KEY,
    CreatedAt DATETIME DEFAULT GETDATE(),
    FirstName VARCHAR(55) NOT NULL,
    LastName VARCHAR(55) NOT NULL,
    Email VARCHAR(255) UNIQUE NOT NULL,
    Password VARCHAR(255),
    IsBlocked BIT DEFAULT 0
);

CREATE TABLE Addresses (
    Id INT IDENTITY(1,1) PRIMARY KEY,
    CustomerId INT NOT NULL,
    AddressLine1 VARCHAR(255) NOT NULL,
    AddressLine2 VARCHAR(255),
    City VARCHAR(255) NOT NULL,
    Country VARCHAR(255) NOT NULL,
    Zipcode VARCHAR(255) NOT NULL,
    IsPrimary BIT DEFAULT 0 NULL,
    FOREIGN KEY (CustomerId) REFERENCES Customers(Id)
);

CREATE TABLE Categories (
    Id INT PRIMARY KEY,
    Name VARCHAR(255) NOT NULL,
    ParentId INT NULL,
    FOREIGN KEY (ParentId) REFERENCES Categories(Id)
);

CREATE TABLE Characteristics (
    Id INT IDENTITY(1,1) PRIMARY KEY,
    Type NVARCHAR(255) NOT NULL,
    Name NVARCHAR(255) NOT NULL
);

CREATE TABLE CategoryCharacteristic (
    CategoryId INT NOT NULL,
    CharacteristicId INT NOT NULL,
    PRIMARY KEY (CategoryId, CharacteristicId),
    FOREIGN KEY (CategoryId) REFERENCES Categories(Id),
    FOREIGN KEY (CharacteristicId) REFERENCES Characteristics(Id)
);

CREATE TABLE Products (
    Id INT IDENTITY(1,1) PRIMARY KEY,
    ProductName VARCHAR(50) NOT NULL,
    ShippingPrice DECIMAL(10,2) NOT NULL,
    SellingPrice DECIMAL(10,2) NOT NULL,
    CategoryId INT NOT NULL,
    FOREIGN KEY (CategoryId) REFERENCES Categories(id)
);

CREATE TABLE ProductCharacteristics (
    ProductId INT NOT NULL,
    CharacteristicId INT NOT NULL,
    PRIMARY KEY (ProductId, CharacteristicId),
    FOREIGN KEY (ProductId) REFERENCES Products(Id),
    FOREIGN KEY (CharacteristicId) REFERENCES Characteristics(Id)
);

CREATE TABLE Orders
(
    OrderId UNIQUEIDENTIFIER PRIMARY KEY,
    CustomerId INT NOT NULL,
    OrderDate DATETIME NOT NULL DEFAULT GETDATE(),
    OrderStatus VARCHAR(30) NOT NULL,
    FOREIGN KEY (CustomerId) REFERENCES Customers(Id),
);

/*
    В различни бази данни като MySQL за стойностите на OrderStatus могат да се използват ENUM -
    OrderStatus ENUM('Shipped', 'Unpaid', 'Payment Approved', 'Prepared to Ship', 'Received') NOT NULL
*/
ALTER TABLE Orders
    ADD CONSTRAINT chk_OrderStatus CHECK (OrderStatus IN ('Shipped', 'Unpaid', 'Payment Approved', 'Prepared to Ship', 'Received'));


CREATE TABLE OrderDetails
(
    OrderId UNIQUEIDENTIFIER NOT NULL,
    ProductId INT NOT NULL,
    Quantity INT NOT NULL,
    FOREIGN KEY (OrderId) REFERENCES Orders(OrderId),
    FOREIGN KEY (ProductId) REFERENCES Products(Id)
);

CREATE TABLE ProductsHistoricalPrices
(
    ProductId INT NOT NULL,
    PriceDate DATETIME NOT NULL,
    ShippingPrice DECIMAL(10,2) NOT NULL,
    SellingPrice DECIMAL(10,2) NOT NULL,
    FOREIGN KEY (ProductId) REFERENCES Products(Id)
);

GO


/*
Този тригер е набор от инструкции, които се изпълняват автоматично от базата данни, когато възникнат определени събития в таблицата „Адреси“. В този случай тригерът се активира след вмъкване на нов ред в таблицата или актуализиране на съществуващ ред.

Тригерът първо задава променлива, наречена "NOCOUNT", на включено, което е настройка за производителност за базата данни. След това декларира няколко променливи, които ще бъдат използвани по-късно в тригера.

След това тригерът създава курсор, който е специален тип цикъл, който позволява на тригера да премине през всеки ред, който е бил засегнат от събитието за вмъкване или актуализиране. За всеки ред тригерът проверява дали стойността на колоната „IsPrimary“ е зададена на 1. Ако е така, тригерът след това актуализира стария адрес по подразбиране за този клиент, като зададе колоната „IsPrimary“ на 0, и актуализира нов ред, който е бил вмъкнат или актуализиран чрез задаване на неговата колона „IsPrimary“ на 1.

Накрая тригерът се затваря и освобождава курсора.

В обобщение, тригерът гарантира, че само един адрес за всеки клиент може да бъде зададен като основен адрес, когато се вмъкне или актуализира нов основен адрес, той ще направи предишния основен адрес вече неосновен.
*/

CREATE TRIGGER tr_Addresses_PrimaryAddress
ON Addresses
AFTER INSERT, UPDATE
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @CustomerId INT;
    DECLARE @IsPrimary BIT;
    DECLARE @Id INT;

    -- loop through each row in the inserted table
    DECLARE cur CURSOR FOR
    SELECT i.CustomerId, i.IsPrimary,i.Id FROM inserted i;
    OPEN cur;
    FETCH NEXT FROM cur INTO @CustomerId, @IsPrimary,@Id;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        IF @IsPrimary = 1
        BEGIN
            -- update the old default address
            UPDATE Addresses
                SET IsPrimary = 0
            WHERE CustomerId = @CustomerId AND IsPrimary = 1;

            -- update the new default address
            UPDATE Addresses
                SET IsPrimary = @IsPrimary
            WHERE Id = @Id;
        END
        FETCH NEXT FROM cur INTO @CustomerId, @IsPrimary,@Id;
    END
    CLOSE cur;
    DEALLOCATE cur;
END;

GO

/*
Този тригер се нарича „tr_ProductsHistoricalPrices_Update“ и е свързан с таблицата „Продукти“. Настроено е да активира събитията „СЛЕД АКТУАЛИЗАЦИЯ“ в таблицата „Продукти“. Когато тригерът е активиран, той настройва „NOCOUNT“ на включено и декларира няколко променливи, включително @ProductId, @OldShippingPrice, @OldSellingPrice.
След това създава курсор, който преминава през всеки ред в "изтритата" таблица и присвоява стойностите за всеки ред на променливите.
За всеки ред той декларира още две променливи @NewSellingPrice и @NewsShippingPrice и присвоява стойностите за съответните колони от „вмъкнатата“ таблица, където ID-ите съвпадат.
Ако старата продажна цена и новата продажна цена или старата цена за доставка и новата цена за доставка са различни, той вмъква старите цени в таблицата ProductsHistoricalPrices с текущата дата и съответния ID на продукта.
След това се затваря и освобождава курсора.

В обобщение, този тригер се използва за проследяване на историческите цени на продуктите. Стартира след направена актуализация на таблицата "Продукти", сравнява старите и новите цени на продукта и ако са различни, ще вмъкне нов ред в таблицата "ПродуктиИсторически цени" със старите цени и текущата дата . По този начин можете да следите как цените на даден продукт са се променили във времето.
*/
CREATE TRIGGER tr_ProductsHistoricalPrices_Update ON Products
AFTER UPDATE
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @ProductId INT;
    DECLARE @OldShippingPrice DECIMAL(10,2);
    DECLARE @OldSellingPrice DECIMAL(10,2);

    DECLARE product_cursor CURSOR FOR
    SELECT Id, ShippingPrice, SellingPrice FROM deleted;

    OPEN product_cursor;
    FETCH NEXT FROM product_cursor INTO @ProductId, @OldShippingPrice, @OldSellingPrice;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        DECLARE @NewSellingPrice DECIMAL(10,2);
        DECLARE @NewsShippingPrice DECIMAL(10,2);
        SELECT @NewSellingPrice = SellingPrice, @NewsShippingPrice = ShippingPrice FROM inserted WHERE Id = @ProductId;

        IF @OldSellingPrice != @NewSellingPrice OR @OldShippingPrice != @NewsShippingPrice
        BEGIN
            INSERT INTO ProductsHistoricalPrices (ProductId, PriceDate, ShippingPrice, SellingPrice)
            VALUES (@ProductId, GETDATE(), @OldShippingPrice, @OldSellingPrice);
        END

        FETCH NEXT FROM product_cursor INTO @ProductId, @OldShippingPrice, @OldSellingPrice;
    END

    CLOSE product_cursor;
    DEALLOCATE product_cursor;
END;

GO

/*
  Тази процедура запазва поръчка за клиент. Първоначално, проверява дали клиентът е блокиран.
  Ако е блокиран, процедурата извежда съобщение за грешка. В противен случай, създава транзакция, записва информацията за поръчката в таблиците Orders,
  OrderDetails и ProductCharacteristics.
  Използва се курсор за да се обходят всички продукти в таблицата @OrderDetails и да се запишат информацията за тях в съответните таблици.
  Ако възникне грешка по време на записването, транзакцията се отменя и извежда се съобщение за грешка.
*/

CREATE TYPE OrderDetailsType AS TABLE
(
    ProductId INT,
    Quantity INT,
    CharacteristicId INT
);

GO

CREATE PROCEDURE SaveOrder
(
    @CustomerId INT,
    @OrderDetails OrderDetailsType READONLY
)
AS
BEGIN
    DECLARE @blocked BIT = 0;

    SELECT @blocked = IsBlocked FROM Customers WHERE Id = @customerId;

    IF @blocked = 1
    BEGIN
        RAISERROR('This customer is blocked and can not make an order', 16, 1);
    END

    BEGIN TRY
        BEGIN TRANSACTION;

        DECLARE @OrderId UNIQUEIDENTIFIER = NEWID();

        INSERT INTO Orders (OrderId, CustomerId, OrderDate, OrderStatus)
            VALUES (@OrderId, @CustomerId, GETDATE(), 'Received');

        DECLARE @ProductId INT;
        DECLARE @Quantity INT;
        DECLARE @CharacteristicId INT;

        DECLARE product_cursor CURSOR FOR
        SELECT ProductId, Quantity, CharacteristicId FROM @OrderDetails;

        OPEN product_cursor;
        FETCH NEXT FROM product_cursor INTO @ProductId, @Quantity, @CharacteristicId;

        WHILE @@FETCH_STATUS = 0
        BEGIN
            INSERT INTO OrderDetails (OrderId, ProductId, Quantity)
                VALUES (@OrderId, @ProductId, @Quantity);

            INSERT INTO ProductCharacteristics (ProductId, CharacteristicId)
                VALUES (@ProductId, @CharacteristicId);

            FETCH NEXT FROM product_cursor INTO @ProductId, @Quantity, @CharacteristicId;
        END

        CLOSE product_cursor;
        DEALLOCATE product_cursor;

    COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH
        ROLLBACK TRANSACTION;
        DECLARE @ErrorMessage NVARCHAR(4000);
        SET @ErrorMessage = 'An error occurred while registering the order. Error number: ' + CONVERT(NVARCHAR, ERROR_NUMBER()) + ', Error message: ' + ERROR_MESSAGE();
        RAISERROR(@ErrorMessage, 16, 1);
    END CATCH
END;

GO

/*
    Въвеждане на данни
*/

INSERT INTO Customers (FirstName, LastName, email, Password, IsBlocked)
VALUES ('John', 'Doe', 'johndoe@example.com', 'password123', 0),
       ('Jane', 'Doe', 'janedoe@example.com', 'password456', 0),
       ('Bob', 'Smith', 'bobsmith@example.com', 'password789', 1);

INSERT INTO Addresses (CustomerId, AddressLine1, AddressLine2, City, Country, Zipcode, IsPrimary)
VALUES (1, 'Primary', 'Apt 4B', 'New York', 'USA', '10001', 1),
       (1, '123 Main St', 'Apt 4B', 'New York', 'USA', '10001', 1),
       (2, '456 Park Ave', NULL, 'Los Angeles', 'USA', '90001', 0),
       (3, '789 Elm St', 'Suite 6C', 'Chicago', 'USA', '60601', 1);

INSERT INTO Categories (Id, ParentId, Name)
VALUES  (1, NULL, 'Electronics'),
        (2, 1, 'Smartphones'),
        (3, 1, 'Laptops'),
        (4, 1, 'Tablets'),
        (5, NULL, 'Clothing'),
        (6, 5, 'Men'),
        (7, 5, 'Women'),
        (8, 5, 'Kids'),
        (9, NULL, 'Sports'),
        (10, 9, 'Outdoor'),
        (11, 9, 'Fitness');

INSERT INTO Characteristics (Type, Name)
VALUES  ('Color', 'Red'),
        ('Size', 'Medium'),
        ('Material', 'Cotton'),
        ('Brand', 'Nike'),
        ('Weight', '1kg');

INSERT INTO CategoryCharacteristic (CategoryId, CharacteristicId)
    VALUES (8, 3);

INSERT INTO Products (ProductName, ShippingPrice, SellingPrice, CategoryId)
VALUES ('Product 1', 10.00, 20.00, 1),
       ('Product 2', 5.00, 15.00, 2),
       ('Product 3', 8.00, 18.00, 3),
       ('Product 4', 12.00, 22.00, 1),
       ('Product 5', 15.00, 25.00, 2),
       ('Product 6', 20.00, 30.00, 3),
       ('Product 7', 30.00, 40.00, 1),
       ('Product 8', 35.00, 45.00, 2),
       ('Product 9', 40.00, 50.00, 3),
       ('Product 10', 50.00, 60.00, 1);
GO

DECLARE @OrderDetails1 OrderDetailsType
INSERT INTO @OrderDetails1 (ProductId, Quantity, CharacteristicId) VALUES (1, 2, 1);
EXEC SaveOrder @CustomerId = 1, @OrderDetails = @OrderDetails1

GO

DECLARE @OrderDetails OrderDetailsType
INSERT INTO @OrderDetails (ProductId, Quantity, CharacteristicId) VALUES (1, 2, 2), (2, 1, 3);
EXEC SaveOrder @CustomerId = 2, @OrderDetails = @OrderDetails

GO

/*
    Изглед, който да връща списък на активните клиенти (тези, които имат
    направени поръчки и не са блокирани).
 */
CREATE VIEW ActiveCustomers
AS
    SELECT c.Id, c.FirstName, c.LastName, c.email FROM Customers c
        INNER JOIN Orders o ON c.Id = o.CustomerId
    WHERE c.IsBlocked = 0;

GO

/*
    UDF, който приема за параметър категория и дата и връща информация за
    поръчаните на или след тази дата продукти от тази категория и съответните поръчки.
 */
CREATE FUNCTION GetProductOrdersByCategoryAndDate (@CategoryId INT, @Date DATETIME)
RETURNS TABLE
AS
RETURN
    (
    SELECT
        Products.Id, Products.ProductName, Orders.OrderDate, Orders.OrderId
    FROM
        Products
    JOIN
        OrderDetails ON Products.Id = OrderDetails.ProductId
    JOIN
        Orders ON OrderDetails.OrderId = Orders.OrderId
    WHERE
        Products.CategoryId = @CategoryId AND Orders.OrderDate >= @Date
    )

GO

/* Пример */

DECLARE @categoryId INT = 1
DECLARE @date DATETIME = '2022-01-01'

SELECT * FROM GetProductOrdersByCategoryAndDate(@categoryId, @date)


/*
   Заявка за най-често поръчваните продукти (бест селърите в магазина)
*/
SELECT p.ProductName, SUM(od.Quantity) as TotalQuantity
FROM Products p
INNER JOIN OrderDetails od ON p.Id = od.ProductId
GROUP BY p.ProductName
ORDER BY TotalQuantity DESC;

GO

/*
   Заявка: по зададен продукт, най-често поръчваните заедно с него продукти
*/

WITH CTE_OrderedProducts AS (
    SELECT ProductId, COUNT(ProductId) AS Quantity
    FROM OrderDetails
    GROUP BY ProductId
)

SELECT p1.ProductName, p2.ProductName, COUNT(od.ProductId) as Quantity
FROM CTE_OrderedProducts op1
JOIN OrderDetails od ON od.ProductId = op1.ProductId
JOIN Products p1 ON p1.Id = od.ProductId
JOIN Products p2 ON p2.Id = od.ProductId
WHERE p1.Id = 1
GROUP BY p1.ProductName, p2.ProductName
ORDER BY Quantity DESC

GO

/*
   Заявка: по зададен продукт, как се е променяла цената му в рамките на послените 30 дни
*/

SELECT
    ProductId,
    PriceDate,
    ShippingPrice,
    SellingPrice,
    LAG(SellingPrice, 1) OVER (ORDER BY PriceDate) as PreviousSellingPrice
FROM
    ProductsHistoricalPrices
WHERE
    ProductId = 1
    AND PriceDate >= DATEADD(day, -30, GETDATE())
ORDER BY
    PriceDate

GO

/*
    Каква е печалбата ни (разликата между продажните цени и доставните цени) за
    зададен период от време, като се взимат предвид само платените поръчки
*/
SELECT SUM(od.Quantity * (p.SellingPrice - p.ShippingPrice)) as Profit
FROM Orders o
JOIN OrderDetails od ON o.OrderId = od.OrderId
JOIN Products p ON od.ProductId = p.Id
WHERE o.OrderStatus = 'Payment Approved' AND o.OrderDate BETWEEN 'start_date' AND 'end_date'

/*
    За тестване на тригърите
*/

UPDATE Products SET SellingPrice = 500
WHERE Id = 1;

UPDATE Products SET ShippingPrice = 200
WHERE Id = 1;

UPDATE Products SET ProductName = 'Product 1 - Changed Price'
WHERE Id = 1;
